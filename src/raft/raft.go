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
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	ELECTION_TIMEOUT_LB = 150
	ELECTION_TIMEOUT_UB = 300

	REQUEST_VOTE_TIMEOUT = 20 * time.Millisecond
	HEART_BEAT_TIMEOUT = 50 * time.Millisecond
	CHECK_LAST_APPLIED_TIMEOUT = 20 * time.Millisecond
	CHECK_REPLICATION_TIMEOUT = 20 * time.Millisecond
	CHECK_MAJORITY_VOTE_TIMEOUT = 20 * time.Millisecond

    RPC_APPEND_ENTRIES = "Raft.AppendEntries"
    RPC_REQUEST_VOTE = "Raft.RequestVote"

    NOBODY = -1

	FOLLOWER = iota
	CANDIDATE
	LEADER
)

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

type logEntry struct {
	Index int
	Term int
	Command interface{}
}

type LeaderMsg struct {
	Term 		int
	LeaderId 	int
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

	// raft p4: Figure 2
	// Persistent State: all servers
	currentTerm 	int			  // latest term server has seen (initialize to 0)
	votedFor 		int			  // candidateId that received vote in current term (or null if none)
	logs 			[]logEntry    // log entries. each entry contains command. Rename to logs because of standard package log

	// Volatile State: all servers
	commitIndex 	int 		  // index of highest log entry known to be committed (initialize to 0)
	lastApplied 	int 		  // index of highest log entry applied (initialize to 0)
	// Volatile State: leaders
	nextIndex 		[]int 		  // index of the next log entry to send to each peer
	matchIndex 		[]int 		  // index of the highest log entry known to be replicated on server (initialize to 0)

	// Other States
	role 			int
	killed 			bool 				// use to suggest this instance has been killed, only for tests
	votes 			[]bool
	rCh 			chan interface{} 	// use to suggest new election timeout
	applyCh 		chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == LEADER
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("invalid persistent data format")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.PDPrintf("read persistent data, term %d, votedFor %d, logs %v",
			rf.currentTerm, rf.votedFor, rf.logs)
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        	int
	CandidateID 	int

	LastLogIndex 	int // index of candidate's last log entry
	LastLogTerm 	int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	var voteGranted bool

	if args.Term == rf.currentTerm {
		if rf.votedFor == NOBODY || rf.votedFor == args.CandidateID {
			voteGranted = rf.checkVoteConsistency(args)
		}
	} else if args.Term > rf.currentTerm {
		voteGranted = rf.checkVoteConsistency(args)
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
	}

	if voteGranted {
		rf.votedFor = args.CandidateID
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted

	// persist state before responding to RPCs
	// term, votedFor may have changed
	rf.persist()
	rf.rCh<-struct{}{}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call(RPC_REQUEST_VOTE, args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderId 		int
	PrevLogIndex 	int 		// index of log entry immediately preceding new ones
	PrevLogTerm 	int 		// term of prevLogIndex entry
	Entries 		[]logEntry 	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 	int 		// Leader's commit index
}

type AppendEntriesReply struct {
	Term 	int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// a candidate may receive an AppendEntries RPC from another
	// server claiming to be leader. If the leader's term is at least
	// as large as the candidate's current term, then the candidate
	// recognizes the leader as legitimate and returns to follower state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm. (Consistency Check)
	var prevIndex int
	var found bool
	if args.PrevLogIndex > 0 {
		prevIndex, found = rf.findLogEntry(args.PrevLogIndex, args.PrevLogTerm)
		if !found {
			reply.Term = rf.currentTerm
			reply.Success = false
			rf.persist()
			rf.rCh<-struct{}{}
			return
		}
	} else {
		prevIndex = -1
	}

	rf.role = FOLLOWER

	// 3. if an existing entry conflicts with a new one(same index
	// but different terms), delete the existing entry and all that
	// follow it
	rf.truncateConflictLogEntries(args.Entries)

	// 4. Append any new entries not already in the log
	//rf.PDPrintf("rf.logs: %v, args.Entries: %v", rf.logs, args.Entries)
	rf.appendNewEntries(args.Entries, prevIndex)

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		le := rf.lastLogEntry()
		rf.commitIndex = min(args.LeaderCommit, le.Index)
	}

	reply.Term = args.Term
	reply.Success = true

	// persists state before responding to RPCs
	// term, logs may be changed
	rf.persist()
	rf.rCh<-struct{}{}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call(RPC_APPEND_ENTRIES, args, reply)
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

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.role == LEADER

	if !isLeader {
		return index, term, false
	} else {
		// raft paper p6
		// appends the command to its log as a new entry.
		// each log entry stores a state machine command
		// alone with the term number.
		ll := rf.lastLogEntry()

		e := logEntry{
			Index:   ll.Index+1,
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.PDPrintf("new log entry: %v", e)

		rf.nextIndex[rf.me] = e.Index+1
		// issues AppendEntries RPCs in parallel to each
		// of the other servers to replicate the entry.
		// if followers crash or run slowly, or if the
		// network packets are lost, the leader retries
		// AppendEntries RPCs indefinitely
		rf.logs = append(rf.logs, e)
		rf.matchIndex[rf.me] = e.Index
		rf.persist()
		return e.Index, rf.currentTerm, true
	}

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
	rf.PDPrintf("I'm killed")
	rf.killed = true
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.initServer()


	// if no communication: begins a new election
	// if receive valid RPCs from a leader or candidate, remains in follower state
	go func() {
		for {
			et := generateET()
			rf.PDPrintf("new election timeout: %d", et)

			select {
			case <-rf.rCh:
			case <-time.After(time.Millisecond * time.Duration(et)): {
				rf.PDPrintf("begins a new election")
				go rf.beginNewElection()
			}
			}
		}
	}()

	go func() {
		for {
			rf.mu.Lock()
			if rf.role == CANDIDATE && hasMajorityVotes(rf.votes) {
				rf.PDPrintf("becomes LEADER")
				rf.role = LEADER
				rf.initNextIndex()
			}
			rf.mu.Unlock()
			time.Sleep(CHECK_MAJORITY_VOTE_TIMEOUT)
		}
	}()

	go rf.sendAppendEntriesMessages()

	go func() {
		for {
			// Raft Paper Figure 2
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] >= N, and log[N].term == currentTerm: set
			// commitIndex = N
			// PURPOSE: split checking logic and replication apart
			rf.mu.Lock()
			if rf.role == LEADER && len(rf.logs) > 0 {
				for _, le := range rf.logs[rf.commitIndex:] {
					// Raft paper p9
					// Raft never commits log entries from previous terms by counting replicas
					if le.Term != rf.currentTerm {
						continue
					}
					if hasSafelyReplicated(rf.matchIndex, le.Index) {
						rf.commitIndex = le.Index
						break
					}
				}
			}
			rf.mu.Unlock()
			time.Sleep(CHECK_REPLICATION_TIMEOUT)
		}
	}()

	go func() {
		for {
			// if commitIndex > lastApplied: increment lastApplied, apply
			// log[lastApplied] to state machine
			//rf.PDPrintf("rf.logs %v, rf.term %v, rf.nextIndex %d, rf.commitIndex %d, rf.lastApplied %d",
			//	rf.logs, rf.currentTerm, rf.nextIndex, rf.commitIndex, rf.lastApplied)
			//rf.PDPrintf("rf.term %v, rf.nextIndex %d, rf.commitIndex %d, rf.lastApplied %d",
			//	rf.currentTerm, rf.nextIndex, rf.commitIndex, rf.lastApplied)
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				le := rf.logs[rf.lastApplied]
				rf.applyCh<-ApplyMsg{
					CommandValid: true,
					Command:      le.Command,
					CommandIndex: le.Index,
				}
				rf.lastApplied += 1
			}
			rf.mu.Unlock()
			time.Sleep(CHECK_LAST_APPLIED_TIMEOUT)
		}
	}()

	return rf
}

func (rf *Raft) initServer() {
	rf.currentTerm = 0
	rf.votedFor = NOBODY
	rf.logs = []logEntry{}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.initNextIndex()
	rf.matchIndex = make([]int, len(rf.peers))

	// raft p5 5.2
	// When servers start up, they begin as followers.
	rf.role = FOLLOWER
	rf.killed = false
	rf.votes = make([]bool, len(rf.peers))
	rf.rCh = make(chan interface{})
	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	// initialization ends
}

func (rf *Raft) initNextIndex() {
	// initialized to leader last log index + 1
	ll := rf.lastLogEntry()
	nci := ll.Index + 1
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = nci
	}
}

func (rf *Raft) beginNewElection() {
	// increase current term
	rf.mu.Lock()

	rf.currentTerm += 1
	rf.votes = make([]bool, len(rf.peers))
	// votes for itself
	rf.votedFor = rf.me
	rf.votes[rf.me] = true
	// transition to candidate state
	rf.role = CANDIDATE
	// should persist state because the votedFor and currentTerm has changed
	rf.persist()

	rf.mu.Unlock()
	// issues RequestVote RPCs in parallel to each of the other servers
	for i, _ := range rf.peers {
		if i != rf.me {
			func(server int) {
				go rf.issueRequestVote(server)
			}(i)
		}
	}
}

func (rf *Raft) issueRequestVote(server int) {
	// Raft uses the voting process to prevent a candidate from winning an election
	// unless its log contains all committed entries. So RequestVoteArgs should take
	// information about the candidate's logs
	rf.mu.Lock()
	ll := rf.lastLogEntry()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: ll.Index,
		LastLogTerm:  ll.Term,
	}
	rf.mu.Unlock()

	// retry if request failed
	for {
		reply := RequestVoteReply{}
		rf.PDPrintf("sends RequestVote to %d", server)
		ok := rf.sendRequestVote(server, &args, &reply)

		if ok {
			rf.PDPrintf("request vote reply %v", reply)

			rf.mu.Lock()
			if args.Term == reply.Term && rf.currentTerm == reply.Term && reply.VoteGranted {
				rf.PDPrintf("receive vote from %d", server)
				rf.votes[server] = true
			} else {
				rf.checkTerm(reply.Term)
			}
			rf.mu.Unlock()

			rf.rCh<-struct{}{}
			return
		} else {
			time.Sleep(REQUEST_VOTE_TIMEOUT)
		}
	}
}

func (rf *Raft) sendAppendEntriesMessages() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int) {
				for {
					// without asserting role, even if turned into FOLLOWER or CANDIDATE,
					if rf.role != LEADER {
						time.Sleep(HEART_BEAT_TIMEOUT)
						continue
					}

					shouldSleep := true

					rf.mu.Lock()
					// the rf node will continue sending append entries to other nodes
					// different peers can receive different entries
					ple := rf.prevLogEntry(server)
					lle := rf.lastLogEntry()
					entries := rf.nextLogEntries(server)
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: ple.Index,
						PrevLogTerm:  ple.Term,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					reply := AppendEntriesReply{}
					//rf.PDPrintf("sends AppendEntries to %d, with entries %v", server, entries)
					rf.PDPrintf("sends AppendEntries to %d, with entries length %d", server, len(entries))
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok {
						rf.mu.Lock()
						if args.Term == reply.Term && rf.currentTerm == reply.Term {
							if reply.Success {
								//rf.PDPrintf("entries %v replicated to %d", entries, server)
								rf.PDPrintf("entries length %d replicated to %d", len(entries), server)
								// when safely replicated, the leader applies the
								// entry to its state machine and returns the result
								// of that execution to the client
								if rf.nextIndex[server] < lle.Index+1 {
									rf.nextIndex[server] = lle.Index+1
								}
								if rf.matchIndex[server] < lle.Index {
									rf.matchIndex[server] = lle.Index
								}
							} else {
								rf.nextIndex[server] -= 1
								shouldSleep = false
							}
						} else {
							rf.checkTerm(reply.Term)
						}
						rf.mu.Unlock()
						rf.rCh<-struct{}{}
					}

					if shouldSleep {
						time.Sleep(HEART_BEAT_TIMEOUT)
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) prevLogEntry(server int) logEntry {
	var le logEntry
	// ni:		index of log entry last committed by server
	// ni-1: 	last committed log entry's index in server.logs
	// 			because our matchIndex is 1-based
	ni := rf.nextIndex[server]
	if ni-2 >= 0 {
		le = rf.logs[ni-2]
	}
	return le
}

func (rf *Raft) lastLogEntry() logEntry {
	var le logEntry
	if len(rf.logs) > 0 {
		le = rf.logs[len(rf.logs)-1]
	}
	return le
}

func (rf *Raft) nextLogEntries(server int) []logEntry {
	var les []logEntry
	ni := rf.nextIndex[server]
	if ni-1 >= 0 {
		les = rf.logs[ni-1:]
	}
	return les
}

func (rf *Raft) checkTerm(term int) bool {
	if term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = NOBODY
		rf.persist()
		return false
	} else {
		return true
	}
}

func (rf *Raft) checkVoteConsistency(args *RequestVoteArgs) bool {
	// election restriction
	// Raft uses the voting process to prevent a candidate from wining
	// an election unless its log contains all committed entries. A candidate
	// must contact a majority of the cluster in order to be elected, which
	// means that every committed entry must be present in at least one of
	// those servers

	// Raft determines which of two logs is more up-to-date by comparing
	// the index and term of the last entries in the logs. If the logs have
	// last entries with different terms, then the log with the later term
	// is more up-to-date. If the logs end with the same term, then whichever
	// log is longer is more up-to-date
	var voteGranted bool
	if len(rf.logs) > 0 && args.LastLogIndex > 0 {
		ll := rf.lastLogEntry()
		rf.PDPrintf("args %v, last log entry: %v\n", args, ll)
		if args.LastLogTerm > ll.Term {
			voteGranted = true
		}

		if args.LastLogTerm == ll.Term && args.LastLogIndex >= ll.Index {
			voteGranted = true
		}
	} else {
		voteGranted = true
	}
	return voteGranted
}

func (rf *Raft) findLogEntry(targetLogIndex, targetLogTerm int) (int, bool) {
	for i, le := range rf.logs {
		if le.Index == targetLogIndex && le.Term == targetLogTerm {
			return i, true
		}
	}
	return -1, false
}

func (rf *Raft) truncateConflictLogEntries(entries []logEntry) {
	// TODO: can be optimized to O(n) time complexity
	p := -1
	for _, le := range entries {
		for i, ole := range rf.logs {
			if ole.Index == le.Index && ole.Term != le.Term {
				p = i
				break
			}
		}
		if p != -1 {
			rf.logs = rf.logs[:p]
			return
		}
	}
}

func (rf *Raft) appendNewEntries(entries []logEntry, prevIndex int) {
	newPos := -1
	for i, le := range entries {
		isNew := true
		if prevIndex + 1 < len(rf.logs) {
			for _, lle := range rf.logs[prevIndex+1:] {
				if lle.Term == le.Term && lle.Index == le.Index {
					isNew = false
					break
				}
			}
		}
		if isNew {
			newPos = i
			break
		}
	}

	if newPos >= 0 {
		rf.logs = append(rf.logs, entries[newPos:]...)
		rf.PDPrintf("replicate log entry from leader, current logs %v", rf.logs)
	}
}

func generateET() int {
	return rand.Intn(ELECTION_TIMEOUT_LB) + (ELECTION_TIMEOUT_UB - ELECTION_TIMEOUT_LB)
}

func hasMajorityVotes(votes []bool) bool {
	total := len(votes)
	count := 0
	for _, vote := range votes {
		if vote {
			count += 1
		}
	}
	return count >= (total / 2 + 1)
}

func hasSafelyReplicated(matchIndex []int, targetIndex int) bool {
	total := len(matchIndex)
	count := 0
	for _, index := range matchIndex {
		if index >= targetIndex {
			count += 1
		}
	}
	return count >= (total / 2 + 1)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) PDPrintf(format string, a ...interface{}) (n int, err error) {
	if rf.killed {
		return
	}

	roleString := ""
	switch rf.role {
	case FOLLOWER:
		roleString = "F"
	case CANDIDATE:
		roleString = "C"
	case LEADER:
		roleString = "L"
	}
	head := fmt.Sprintf("[Term %d Role %s Peer %d]: ", rf.currentTerm, roleString, rf.me)
	DPrintf(head + format, a...)
	return
}