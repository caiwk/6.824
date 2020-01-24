package raft

import (
	"log_manager"
	"time"
)

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (r RequestVoteReply) Success() bool {
	return r.VoteGranted
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	//Entries[]
	LeaderCommit int
	Entries      []*Entry
}
type AppendEntriesReply struct {
	Term         int
	Inconsistent bool
	Succes       bool
}

func (r AppendEntriesReply) Success() bool {
	return r.Succes
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) > 0 {
		if args.PrevLogIndex > 0 {
			log.Infof("server: %d , get append entris, preindex: %d, preterm: %d , my log_last_index : %d , last_term: %d  ",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term)
		}else{
			log.Infof("server: %d , get append entris, preindex: %d, preterm: %d   ",
				rf.me, args.PrevLogIndex, args.PrevLogTerm)
		}
	}
	//if len(args.Entries) == 0 {
	//	log.Infof("%d , leader's commit index: %d, my commit index: %d" ,
	//		rf.me, args.LeaderCommit, rf.commitIndex)
	//}
	reply.Term = rf.currentTerm
	rf.votedFor = -1
	if args.Term < rf.currentTerm {
		reply.Succes = false
		log.Info("args.s term less than me", rf.me)
		return
	}
	rf.setIsLeader(false)
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].Index != args.PrevLogIndex {
			reply.Succes = false
			reply.Inconsistent = true
			log.Info("inconsistent", rf.me, args.PrevLogIndex)
			return
		}
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Succes = false
			reply.Inconsistent = true
			log.Info(rf.me,"refuse")
			return
		}
	}
	rf.resetTimeout()

	reply.Succes = true
	rf.isLostLeader = false
	//if len(args.Entries) == 0 {
	//	return
	//}
	rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
	// 更新自己的rf.commit,并apply
	old := rf.commitIndex
	if old == 0 {
		old = 1
	}
	if args.LeaderCommit > rf.commitIndex {
		//log.Info(args.LeaderCommit,rf.commitIndex, len(rf.log))
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}
	for i := old - 1 ; i >= 0 && i < rf.commitIndex; i ++ {
		ent := rf.log[i]
		log.Info(rf.me, "apply", ent.Cmd)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      ent.Cmd,
			CommandIndex: ent.Index,
		}
	}
	reply.Inconsistent = false
	return
}
func (rf *Raft) resetTimeout() {
	go func() {
		rf.timeout <- true
	}()
}

func (rf *Raft) PreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetTimeout()
	//log.Info(" %d get prevote req, ")
	if !rf.isLostLeader {
		reply.VoteGranted = false
		return
	}
	if args.Term >= rf.currentTerm || args.LastLogIndex >= rf.lastLogIndex() {
		reply.VoteGranted = true
	}
	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	log.Info(rf.votedFor,args.LastLogIndex ,rf.commitIndex)
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
//age simulates a lossy network, in which servers
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
func (rf *Raft) sendPreVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, res chan int) bool {
	ok := rf.peers[server].Call("Raft.PreVote", args, reply)
	res <- server
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, res chan int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	res <- server
	return ok
}
func (rf *Raft) sendAppendEntries(server int, reply *AppendEntriesReply, ch chan int,lastIndex int) bool {
	args := rf.newAppendEntries(server,lastIndex)
	var ok bool
	for {
		if len(args.Entries) != 0 {
			log.Infof("%d start send append entry to %d, index: %d , length : %d",
				rf.me, server, args.Entries[0].Index, len(args.Entries))
			//for _,v := range args.Entries{
			//	log.Infof("%d send to %d , cmd : %d ", rf.me, server, v.Cmd)
			//}
		}

		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !reply.Succes && reply.Inconsistent {
			log.Info(rf.me, "get inconsistent res from", server)
			rf.mu.Lock()
			rf.nextIndex[server] -= 1
			rf.mu.Unlock()
			if rf.nextIndex[server] < 1 {
				break
			}
			args.Entries = rf.log[rf.nextIndex[server]-1:]
			args.PrevLogIndex = args.Entries[0].Index - 1
		} else {
			if reply.Succes && len(args.Entries) > 0  {
				rf.mu.Lock()
				rf.nextIndex[server] = args.Entries[len(args.Entries)- 1].Index + 1
				rf.mu.Unlock()
			}
			break
		}
	}
	ch <- server
	return ok
}

type rpcRes interface {
	Success() bool
}

func gather(timeout int, res interface{}, servers int, need int, ch chan int) bool {
	timer := time.After(time.Duration(timeout) * time.Millisecond)
	count := 0
	for {
		select {
		case <-timer:
			return false
		case k := <-ch:
			switch x := res.(type) {
			case []*RequestVoteReply:
				if x[k].Success() {
					count++
				}
			case []*AppendEntriesReply:
				if x[k].Success() {
					count++
				}
			}

		}
		if count >= need {
			return true
		}
		if count == servers {
			return false
		}
	}
}
