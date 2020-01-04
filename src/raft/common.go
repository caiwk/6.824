package raft
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) lastLogIndex() int  {

	if len(rf.log) > 0{
		return rf.log[len(rf.log) - 1 ].Index
	}
	return -1
}
func (rf *Raft) lastLog() *Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[rf.lastLogIndex() - 1]
}