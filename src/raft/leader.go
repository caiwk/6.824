package raft

import (
	"log_manager"
	"time"
)

//每个leader都会有这个groutine， 每一段时间就会发送心跳，如果接收到有新的客户端请求，就把日志replicate到其他节点
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
//每个broadcast都是锁住的，
func (rf *Raft) broadcast(heart bool) {
	rf.bclock.Lock()
	defer rf.bclock.Unlock()
	if _, bo := rf.GetState(); !bo {
		return
	}
	if !heart && rf.commitIndex >= len(rf.Log){
		return
	}
	ch := make(chan int, len(rf.peers)-1)
	replySlice := make([]*AppendEntriesReply, len(rf.peers))
	rf.mu.Lock()
	lastLogIndex := rf.lastLogIndex()
	log.Info("leader :",rf.me,"lastlogIndex:", lastLogIndex)
	for k := range rf.peers {
		if rf.me != k {
			replySlice[k] = new(AppendEntriesReply)
			go rf.sendAppendEntries(k, replySlice[k], ch, lastLogIndex)
		}
	}
	rf.mu.Unlock()
	//rf.resetTimeout()
	if rf.gather(300, replySlice, len(rf.peers), len(rf.peers)/2, ch) {
		rf.resetTimeout()
		if !rf.IsLeader(){
			return
		}
		old := rf.commitIndex
		rf.mu.Lock()
		//注意只有在复制日志且日志最后一个entry是leader的term 的时候，才能commit，不然会出现figure8的错误情况
		if !heart && rf.Log[lastLogIndex -1].Term == rf.CurrentTerm{
			rf.commitIndex = lastLogIndex
		}
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
//每次new append entry的时候，都会把nextindex之后所有的log全部发出去，而不是一个一个发
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
	if rf.nextIndex[server] <= len(rf.Log) && lastIndex >= 0 && lastIndex <= len(rf.Log) &&lastIndex >rf.nextIndex[server]-1  {
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
		LastLogTerm:  rf.lastLogTerm(),
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
	if rf.gather(400, replySlice, len(rf.peers), len(rf.peers)/2, res) {
		return true
	}
	return false
}

// 先做prevote，prevote过了的话才正式开始start election，这么做是为了防止脑裂的情况
func (rf *Raft) startElection() (is bool) {
	//oldterm := rf.CurrentTerm
	defer func() {
		if is {
			rf.persist()
			//rf.mu.Lock()
			//rf.CurrentTerm = oldterm
			//rf.mu.Unlock()
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
	if rf.gather(400, replySlice, len(rf.peers), len(rf.peers)/2, res) {
		if rf.VoteFor == rf.me {
			return true
		}
		return false
	}
	rf.mu.Lock()
	rf.VoteFor = -1
	rf.mu.Unlock()

	return false
}

func (rf *Raft) initNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for k := range rf.nextIndex {
		rf.nextIndex[k] = len(rf.Log) + 1
	}
}
