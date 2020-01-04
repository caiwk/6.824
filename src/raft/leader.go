package raft

import (
	log "log_manager"
	"time"
)

func (rf *Raft) heartBeatLoop() {
	for {
		if _, bo := rf.GetState(); !bo  {
			return
		}
		select {
		case <- rf.shutdownCh:
			return
		case <-rf.entryCh:
			go rf.broadcast(false)
		case <-time.After(150 * time.Millisecond):
			go rf.broadcast(true)
		}
	}
}
func (rf *Raft) broadcast(heart bool ) {
	if _, bo := rf.GetState(); !bo  {
		return
	}
	ch := make(chan int, len(rf.peers) - 1)
	replySlice := make([]*AppendEntriesReply, len(rf.peers))
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(AppendEntriesReply)
			go rf.sendAppendEntries(k, replySlice[k],ch)
		}
	}
	rf.resetTimeout()
	if !heart && gather(200, replySlice, len(rf.peers), len(rf.peers)/2, ch) {
		rf.commitIndex = rf.lastLog().Index
		log.Info(rf.me,"apply",rf.log[rf.lastLogIndex() - 1].Cmd)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.lastLog().Cmd,
			CommandIndex: rf.lastLog().Index,
		}
	}
}
func (rf *Raft) newAppendEntries(server int) *AppendEntries {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntries{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		LeaderCommit: rf.commitIndex,
	}
	if args.PrevLogIndex > 0{
		args.PrevLogTerm = rf.log[args.PrevLogIndex - 1].Term
	}
	if rf.nextIndex[server]  <= len(rf.log){
		args.Entries = rf.log[rf.nextIndex[server]-1:]
	}
	return args
}

func ( rf *Raft) startPreVote() bool{
	rf.mu.Lock()
	rf.isLostLeader = true
	rf.setIsLeader(false)
	log.Info(rf.me, "start prevote", rf.currentTerm)
	votereq := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  rf.currentTerm,
	}
	replySlice := make([]*RequestVoteReply, len(rf.peers))
	rf.mu.Unlock()
	rf.resetTimeout()
	res := make(chan int, len(rf.peers)-1)
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(RequestVoteReply)
			go rf.sendPreVote(k, votereq, replySlice[k], res)
		}
	}
	if gather(200, replySlice, len(rf.peers),len(rf.peers) /2 ,res){
		return true
	}
	return false
}
func (rf *Raft) startElection() bool {
	if !rf.startPreVote(){
		return false
	}
	rf.mu.Lock()
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
	rf.mu.Unlock()
	rf.resetTimeout()
	res := make(chan int, len(rf.peers)-1)
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(RequestVoteReply)
			go rf.sendRequestVote(k, votereq, replySlice[k], res)
		}
	}
	if gather(200, replySlice, len(rf.peers),len(rf.peers) /2 ,res){
		if rf.votedFor == rf.me {
			return true
		}
		return false
	}
	rf.mu.Lock()
	rf.votedFor = -1
	rf.mu.Unlock()
	return false
}

func (rf *Raft) initNextIndex(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for k := range rf.nextIndex{
		rf.nextIndex[k] = len(rf.log) + 1
	}
}