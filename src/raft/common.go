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

	if len(rf.Log) > 0{
		return rf.Log[len(rf.Log) - 1 ].Index
	}
	return 0
}
func (rf *Raft) lastLog() *Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Log[rf.lastLogIndex() - 1]
}