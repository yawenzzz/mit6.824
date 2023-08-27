package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/labrpc"

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有servers的状态 (Persistent)
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateID
	log         []LogEntry // log entries; 每个条目包含状态机的命令，以及条目被leader接收的Term // (first index is 1)
	// 所有servers的状态 (Volatile)
	commitIndex int // 已知已提交的最高日志项的索引 (initialized to 0, increases monotonically)
	lastApplied int // 应用于状态机的最高日志条目的索引
	// leaders的状态(Volatile)
	nextIndex  []int // 对于每个服务器，要发送到该服务器的下一个日志条目的索引(初始化为leader的最后一个日志索引1)
	matchIndex []int // 对于每个服务器，已知要在服务器上复制的最高日志条目的索引
	// 其他变量
	State           State // 节点的状态
	ElectionTimeOut time.Duration
	Ticker          *time.Ticker
	// 消息通道
	ReceiveHeartBeat chan bool
	VoteDone         chan bool
	IsLeader         chan bool
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Term和指令
type LogEntry struct {
	Term         int
	Instructions interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.State == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 竞选者的任期
	CandidateId  int // 要求投票的candidate
	LastLogIndex int // candidate的最后一个log entry的索引
	LastLogTerm  int // candidate最后一个log entry的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前的Term，用于candidate更新自己
	VoteGranted bool // 如果是true则说明candidate收到了vote
}

// example RequestVote RPC handler.
// 投票的逻辑
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// 如果args.term < currentTerm 返回false，不投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.VoteDone <- true
		rf.mu.Unlock()
		return
	}
	// 每一个服务器最多会对一个任期号投出一张选票，按照先来先服务的原则
	// 如果args.term == currentTerm
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		// 如果对于同一个任期已经投过票则返回false
		if rf.votedFor != -1 {
			reply.VoteGranted = false
			rf.VoteDone <- true
			rf.mu.Unlock()
			return
		}
	}
	// 剩下的情况可以投票
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	// 给某个候选人投了票，自己就变成候选人
	rf.State = Candidate
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.VoteDone <- true
	fmt.Println("server ", rf.me, "投给了", rf.votedFor)
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// 如果不成功则一直发送
	for !ok {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			break
		}
	}
	rf.mu.Lock()
	// 统计票数
	if reply.VoteGranted {
		*voteNum++
		fmt.Println("收到票数，现在是", *voteNum)
	}
	if *voteNum > len(rf.peers)/2 {
		*voteNum = 0
		rf.State = Leader
		_, isleader := rf.GetState()
		rf.IsLeader <- isleader
		rf.nextIndex = make([]int, len(rf.peers))
		for idx := range rf.nextIndex {
			rf.nextIndex[idx] = len(rf.log)
		}
		rf.mu.Unlock()
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int // leader的term
	leaderId     int // follower可以重定向客户
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // 如果是heartbeat就是空的，为了提高效率一次可以多发送几个
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm 用于leader更新自己的Term
	Success bool // 如果follower包含匹配prevLogIndex和prevLogTerm的条目，则为true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// 对于follower来说，如果收到的消息任期要小于自己当前的任期则拒绝添加entry
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.leaderId
	rf.State = Follower
	// 表示接受者接受到了Heartbeat
	rf.ReceiveHeartBeat <- true
	// reply
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.mu.Unlock()

}

// 需要判断存活的服务器有多少
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// 如果不成功则一直发送
	for !ok {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			break
		}
	}
	rf.mu.Lock()
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 定义心跳频率
const HeartBeatTime = 180 * time.Millisecond

func (rf *Raft) Loop() {
	for rf.killed() == false {
		switch rf.State {
		case Follower:
			select {
			case <-rf.ReceiveHeartBeat:
				fmt.Println("Server ", rf.me, "收到了心跳")
			case <-rf.VoteDone:
				fmt.Println("Server ", rf.me, "完成了投票")
			case <-time.After(HeartBeatTime):
				fmt.Println("Server ", rf.me, "变成candidate了")
				rf.State = Candidate
			}
		case Candidate:
			// 如果是candidate就设置选举超时时间, 每一轮重新设置
			// 自增当前的任期号（currentTerm）
			currentTerm := rf.currentTerm
			rf.ElectionTimeOut = time.Duration(rand.Intn(180)+180) * time.Millisecond
			// 给自己投票
			rf.votedFor = rf.me
			// 统计选票的数量
			voteNum := 1
			for name := range rf.peers {
				if name == rf.me {
					continue
				}
				args := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,            // 上一个Index
					LastLogTerm:  rf.log[len(rf.log)-1].Term, // 上一个任期
				}
				reply := new(RequestVoteReply)
				// 发送请求投票的 RPC 给其他所有服务器
				go rf.sendRequestVote(name, args, reply, &voteNum)
			}
			// 进入非阻塞状态，如果在选举期间内接受到了心跳，则自动成为Follower
			// 如果选举超时，重新选举
			select {
			case <-rf.ReceiveHeartBeat:
				rf.State = Follower
			case <-rf.IsLeader:
				fmt.Println("选举出了Leader，Leader是", rf.me)
				//break
			case <-time.After(50 * rf.ElectionTimeOut):
				fmt.Println("server ", rf.me, "时间： ", 50*rf.ElectionTimeOut, "选举超时，重新选举")
				// 等待超时的时间
			}
			//time.Sleep(rf.ElectionTimeOut)
		case Leader:
			rf.Ticker = time.NewTicker(HeartBeatTime)
			// 定时发送heartbeat
			select {
			case <-rf.Ticker.C:
				//fmt.Println("Term: ", rf.currentTerm)
				for name := range rf.peers {
					if name == rf.me {
						continue
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						leaderId:     rf.me,
						PrevLogIndex: len(rf.log) - 1,
						PrevLogTerm:  rf.log[len(rf.log)-1].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					reply := new(AppendEntriesReply)
					go rf.SendAppendEntries(name, &args, reply)
				}
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Term:         1,
		Instructions: nil,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.State = Follower
	rf.ReceiveHeartBeat = make(chan bool, 10)
	rf.VoteDone = make(chan bool, 10)
	rf.IsLeader = make(chan bool, 10)
	//rf.ElectionTimeOut = time.Duration(rand.Intn(180)+180) * time.Millisecond

	//fmt.Println(len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Loop()
	return rf
}
