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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Fllower
	rf.currentTerm = args.Term
	rf.electionTimer.Reset(rf.GetElectionOutTime())
	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) BroadcastHeartBeat() {
	term := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := rf.commitIndex
	prevLogTerm := rf.currentTerm
	leaderCommit := rf.lastApplied
	rf.electionTimer.Reset(rf.GetElectionOutTime())

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		args := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
		args.Term = term
		args.LeaderId = leaderId
		args.PrevLogTerm = prevLogTerm
		args.PrevLogIndex = prevLogIndex
		args.LeaderCommit = leaderCommit
		go func(peer int) {
			rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		}(i)

	}
}
