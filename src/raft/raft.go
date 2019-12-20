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
	log "log_manager"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	timeout  chan bool
	isleader bool
	isKill   bool
	//persistent state on all servers
	currentTerm uint64
	votedFor    int
	log         []byte
	//volatile state on all servers
	commitIndex uint64
	lastApplid  uint64
	// volatile state on leaders
	nextIndex  []uint64
	matchIndex []uint64
}

func (rf *Raft) IsLeader() bool {
	return rf.isleader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	//var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm)

	return term, rf.IsLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint64
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint64
	VoteGranted bool
}

type AppendEntries struct {
	Term         uint64
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	//Entries[]
	LeaderCommit uint64
	Entries      []byte
}

type AppendEntriesReply struct {
	Term   uint64
	Succes bool
}

func min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	rf.votedFor = -1
	if args.Term < rf.currentTerm {
		reply.Succes = false
	}
	rf.setIsLeader(false)
	reply.Succes = true
	rf.resetTimeout()
	//todo 2
	//todo 3
	//todo 4
	//todo 5
	//if args.LeaderCommit > rf.commitIndex{
	//
	//	rf.commitIndex = min(rf.commitIndex, )
	//}
}
func (rf *Raft) resetTimeout() {
	go func() {
		rf.timeout <- true
	}()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.Term > rf.currentTerm && args.LastLogIndex >= rf.commitIndex {
		rf.setIsLeader(false)
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.resetTimeout()
	}
	log.Info(rf.me, " return ", reply.VoteGranted, args.CandidateId, rf.votedFor, args.Term, rf.currentTerm)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, res chan int) bool {
	//log.Info("start send request vote from", rf.me," to ",server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//log.Info("get vote res from",server," to ",rf.me)
	//if !ok{
	//	log.Info(rf.me,"get vote from",server," fail")
	//}

	res <- server

	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	log.Info("kill", rf.me)
	rf.setIsLeader(false)
	rf.isKill = true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.timeout = make(chan bool, 10)
	rf.votedFor = -1
	// Your initialization code here (2A, 2B, 2C).
	log.Info("make new raft peer", rf.me)
	go rf.rpcHandleLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) rpcHandleLoop() {
	for {
		if rf.isKill {
			log.Info(rf.me, "killed")
			return
		}
		timeout := rand.Intn(100) + 200 // 300ms ~ 500ms

		select {

		case <-rf.timeout:
			break
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			go func() {
				if rf.startElection() {
					if !rf.isleader {
						rf.setIsLeader(true)
						log.Infof("%d become leader !!! ", rf.me)
						go rf.heartBeatLoop()
					}
				}
			}()
		}
	}
}
func (rf *Raft) setIsLeader(is bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isleader = is
}
func (rf *Raft) heartBeatLoop() {
	for {
		if _, bo := rf.GetState(); !bo {
			break
		}
		beatReq := &AppendEntries{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.commitIndex,
			PrevLogTerm:  rf.currentTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      nil,
		}
		replySlice := make([]*AppendEntriesReply, len(rf.peers))
		for k := range rf.peers {
			if rf.me != k {
				replySlice[k] = new(AppendEntriesReply)
				go rf.sendAppendEntries(k, beatReq, replySlice[k])
			}
		}
		rf.resetTimeout()
		time.Sleep(150 * time.Millisecond)
	}

}
func (rf *Raft) startElection() bool {

	rf.setIsLeader(false)
	rf.currentTerm += 1
	log.Info(rf.me, "start election", rf.currentTerm)
	votereq := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  rf.currentTerm,
	}
	replySlice := make([]*RequestVoteReply, len(rf.peers))
	rf.votedFor = rf.me
	rf.resetTimeout()
	res := make(chan int, len(rf.peers)-1)
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(RequestVoteReply)
			go rf.sendRequestVote(k, votereq, replySlice[k], res)
		}
	}
	total := 0
	Grantcount := 0
	for i := range res {
		total++
		if replySlice[i].VoteGranted {
			Grantcount++
		}
		if Grantcount+1 > len(rf.peers)/2 {
			return true
		}
		if total == len(rf.peers)-1 {
			rf.votedFor = -1
			return false
		}
	}
	return false

}
