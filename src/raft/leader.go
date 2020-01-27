package raft

import (
	"log_manager"
	"time"
)

func (rf *Raft) heartBeatLoop() {
	for {
		if _, bo := rf.GetState(); !bo {
			return
		}
		select {
		case <-rf.shutdownCh:
			return
		case <-rf.entryCh:
			go rf.broadcast(false)
		case <-time.After(150 * time.Millisecond):
			go rf.broadcast(true)
		}
	}
}
func (rf *Raft) broadcast(heart bool) {
	rf.bclock.Lock()
	defer rf.bclock.Unlock()
	if _, bo := rf.GetState(); !bo {
		return
	}
	ch := make(chan int, len(rf.peers)-1)
	replySlice := make([]*AppendEntriesReply, len(rf.peers))
	rf.mu.Lock()
	lastLogIndex := rf.lastLogIndex()
	log.Info(rf.me, lastLogIndex)
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(AppendEntriesReply)
			go rf.sendAppendEntries(k, replySlice[k], ch, lastLogIndex)
		}
	}
	rf.mu.Unlock()

	if gather(200, replySlice, len(rf.peers), len(rf.peers)/2, ch) {
		rf.resetTimeout()
		old := rf.commitIndex
		rf.mu.Lock()
		rf.commitIndex = lastLogIndex
		rf.mu.Unlock()
		for i := old; i < rf.commitIndex; i ++ {
			log.Info(rf.me, "apply", rf.Log[i].Cmd, " commit index : ", rf.commitIndex)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Cmd,
				CommandIndex: rf.Log[i].Index,
			}
		}
	}
}
func (rf *Raft) newAppendEntries(server int, lastIndex int) *AppendEntries {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntries{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		LeaderCommit: rf.commitIndex,
	}
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.Log[args.PrevLogIndex-1].Term
	}
	if rf.nextIndex[server] <= len(rf.Log) && lastIndex >= 0 && lastIndex <= len(rf.Log) {
		//log.Infof("send to %d , %d, %d , %d  ",server, len(rf.Log), lastIndex,rf.nextIndex[server]-1)
		args.Entries = rf.Log[rf.nextIndex[server]-1 : lastIndex]
	}
	return args
}

func (rf *Raft) startPreVote() bool {
	rf.mu.Lock()
	rf.isLostLeader = true
	rf.setIsLeader(false)
	rf.VoteFor = -1
	rf.mu.Unlock()
	log.Info(rf.me, "start prevote", rf.CurrentTerm)
	votereq := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.CurrentTerm,
	}
	replySlice := make([]*RequestVoteReply, len(rf.peers))

	rf.resetTimeout()
	res := make(chan int, len(rf.peers)-1)
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(RequestVoteReply)
			go rf.sendPreVote(k, votereq, replySlice[k], res)
		}
	}
	if gather(200, replySlice, len(rf.peers), len(rf.peers)/2, res) {
		return true
	}
	return false
}
func (rf *Raft) startElection() (is bool) {
	defer func() {
		if is {
			rf.persist()
		}
	}()
	if !rf.startPreVote() {
		return false
	}
	rf.mu.Lock()
	rf.setIsLeader(false)
	rf.CurrentTerm += 1
	log.Info(rf.me, "start election", rf.CurrentTerm)
	votereq := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.CurrentTerm,
	}
	replySlice := make([]*RequestVoteReply, len(rf.peers))
	rf.VoteFor = rf.me
	rf.mu.Unlock()
	rf.resetTimeout()
	res := make(chan int, len(rf.peers)-1)
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(RequestVoteReply)
			go rf.sendRequestVote(k, votereq, replySlice[k], res)
		}
	}
	if gather(200, replySlice, len(rf.peers), len(rf.peers)/2, res) {
		if rf.VoteFor == rf.me {
			return true
		}
		return false
	}
	rf.mu.Lock()
	rf.VoteFor = -1
	rf.mu.Unlock()
	log.Info("start election return false")
	return false
}

func (rf *Raft) initNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for k := range rf.nextIndex {
		rf.nextIndex[k] = len(rf.Log) + 1
	}
}
