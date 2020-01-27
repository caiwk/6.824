package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	log "log_manager"
	"math/rand"
	"sync"
	"time"
)
import (
	"labrpc"
	"bytes"
	"labgob"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
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
	bclock sync.Mutex
	applyCh      chan ApplyMsg
	entryCh      chan bool
	timeout      chan bool
	isleader     bool
	isLostLeader bool
	shutdownCh   chan bool
	//persistent state on all servers
	CurrentTerm int
	VoteFor     int
	Log         []*Entry
	//volatile state on all servers
	commitIndex int
	lastApplid  int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}
func (rf *Raft) IsLeader() bool {
	return rf.isleader
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	//var isleader bool
	// Your code here (2A).
	term = int(rf.CurrentTerm)

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
	rf.mu.Lock()
	rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//Your code here (2C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var logs []*Entry
	if d.Decode(&term) != nil ||
	   d.Decode(&voteFor) != nil|| d.Decode(&logs) != nil {
	  log.Error("read persist decode err")
	} else {
	  rf.CurrentTerm = term
	  rf.VoteFor = voteFor
	  rf.Log = logs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type Entry struct {
	Cmd   interface{}
	Term  int
	Index int
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).
	defer rf.persist()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isleader == false {
		return -1, -1, false
	}
	log.Info(rf.me, "start", command)
	ent := &Entry{
		Cmd:   command,
		Term:  rf.CurrentTerm,
		Index: len(rf.Log) + 1,
	}
	rf.Log = append(rf.Log, ent)
	rf.entryCh <- true
	return len(rf.Log), rf.CurrentTerm, rf.isleader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here, if desired.
	//Log.Info("kill", rf.me)
	rf.setIsLeader(false)
	close(rf.shutdownCh)
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
	rf.VoteFor = -1
	rf.entryCh = make(chan bool, 1)
	rf.Log = make([]*Entry, 0, 8)
	rf.applyCh = applyCh
	rf.shutdownCh = make(chan bool, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.isLostLeader = true
	rf.commitIndex = 0
	rf.bclock = sync.Mutex{}
	// Your initialization code here (2A, 2B, 2C).
	go rf.rpcHandleLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) rpcHandleLoop() {
	for {

		timeout := rand.Intn(100) + 200 // 300ms ~ 500ms

		select {
		case <-rf.shutdownCh:
			return
		case <-rf.timeout:
			break
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			go func() {
				if rf.startElection() {
					rf.mu.Lock()
					rf.setIsLeader(true)
					rf.isLostLeader = false
					rf.mu.Unlock()
					log.Infof("%d become leader !!! ", rf.me)
					rf.initNextIndex()
					go rf.heartBeatLoop()
				}
			}()
		}
	}
}
func (rf *Raft) setIsLeader(is bool) {
	rf.isleader = is
}
