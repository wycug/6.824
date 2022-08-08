package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Entries == nil {
		rf.heartbeatTimer.Reset(rf.GetHeartBeatOutTime())
	}
	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) SendHeatBeat() {
	term := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := rf.commitIndex
	prevLogTerm := rf.currentTerm
	leaderCommit := rf.lastApplied
	for i := 0; i < len(rf.peers); i++ {
		args := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
		args.Term = term
		args.LeaderId = leaderId
		args.PrevLogTerm = prevLogTerm
		args.PrevLogIndex = prevLogIndex
		args.LeaderCommit = leaderCommit
		rf.peers[i].Call("Raft.AppendEntries", args, reply)
	}
}
