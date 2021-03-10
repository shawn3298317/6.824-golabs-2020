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
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state (RWLock to boost performance)
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state fields
	currentTerm int
	votedFor    *int // could be nil
	log	        []*LogEntry // to store state machine commands
	state       State
	voteCount   int

	// Volatile state fields
	commitIndex int
	lastApplied int
	
	// Leader-only state fields
	nextIndex  []int // next log index the leader sends to follower
	matchIndex []int
	
	// non-exported fields
	electionTimeout    time.Duration
	electionTimerReset chan struct{}
	applyMsgInterrupt  chan struct{}
	heartbeatInterval  time.Duration
	applyMsgCh         chan ApplyMsg // TODO: check if we need this????
	
	// debug
	logger     *log.Entry

}

type State int
const (
	FOLLOWER = iota
	LEADER
	CANDIDATE
)

func (s State) String() string {
	switch s {
	case FOLLOWER:
		return "Follower"
	case LEADER:
		return "Leader"
	case CANDIDATE:
		return "Candidate"
	default:
		return "N/A"
	}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	
	return term, isleader
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Terms         int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoteGranted  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.RLock()
	currentTerm := rf.currentTerm
	rf.mu.RUnlock()
	
	if args.Terms < currentTerm {
		rf.logger.Debugf("Peer(%d) term smaller than currentTerm (%d < %d)", args.CandidateId, args.Terms, rf.currentTerm)
		reply.Term = currentTerm
		reply.VoteGranted = false
	} else {
		rf.mu.Lock()
		// updating stale term no.
		if args.Terms > currentTerm {
			rf.logger.Debugf("Peer(%d) term larger than currentTerm (%d > %d)", args.CandidateId, args.Terms, rf.currentTerm)
			rf.currentTerm = args.Terms
			// starting a new term
			rf.votedFor = nil
			rf.voteCount = 0
		}
		
		// Granting votes
		// TODO: check if incoming candidate's log & term is as up-to-date as server
		// reject vote if server is more up-to-date
		if rf.votedFor == nil {
			rf.logger.Debugf("Server hasn't vote before, Voting for %d, Resetting election timer...", args.CandidateId)
			rf.electionTimerReset <- struct{}{}
			rf.votedFor = &args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			
		} else {
			rf.logger.Debug("Server has voted already. Rejecting vote")
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
		rf.mu.Unlock()
		
	}
	return
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == CANDIDATE {
		if reply.Term < rf.currentTerm {
			rf.logger.Debugf("Peer(%d) term smaller than currentTerm (%d < %d)", server, reply.Term, rf.currentTerm)
		} else if reply.Term > rf.currentTerm {
			// revert to follower state when discover currentTerm is obselete
			rf.logger.Debugf("Peer(%d) term larger than currentTerm (%d > %d)", server, reply.Term, rf.currentTerm)
			rf.logger.Debugf("Reverting to FOLLOWER state")
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = nil
			rf.voteCount = 0
			// TODO: check if we need to refresh electionTimer here
			rf.electionTimerReset <- struct{}{}
		} else {
			if reply.VoteGranted {
				rf.voteCount += 1
				rf.logger.Debugf("Received vote from peer(%d). Total vote (%d/%d)", server, rf.voteCount, len(rf.peers))
			}
			// Claim leadership when received majority votes
			if rf.voteCount >= (len(rf.peers) - 1)/2 {
				rf.logger.Debugf("Got total vote: %d >= %d, candidate claiming leadership", rf.voteCount, len(rf.peers)/2)
				rf.state = LEADER
				rf.votedFor = nil
				rf.voteCount = 0
				rf.electionTimerReset <- struct{}{}
				// TODO: check if heartbeats are send out immediately after winning election?
				
				// init nextIndex to leader lastlogIndex + 1;
				// init matchIndex to 0s
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
				}
			}
		}
	} else {
		rf.logger.WithFields(log.Fields{
			"state": rf.state,
		}).Debug("Got reply from RequestVoteRPC, but not in candidate mode already")
	}
	return ok
}


type AppendEntriesArgs struct {
	Term 	 	 int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
}

// RPC handler for handling heartbeats and log append commands from leader(s)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	
	/* TODO:
	1. Reply false if term < currentTerm (§5.1)
	2. Reply false if log doesn’t contain an entry at prevLogIndex
	whose term matches prevLogTerm (§5.3)
	3. If an existing entry conflicts with a new one (same index
	but different terms), delete the existing entry and all that
	follow it (§5.3)
	4. Append any new entries not already in the log
	5. If leaderCommit > commitIndex, set commitIndex =
	min(leaderCommit, index of last new entry)
	*/
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true // default to true
	
	// Match term no.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	
	// Check if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if (args.PrevLogIndex != -1) &&
	   (args.PrevLogIndex) <= len(rf.log)-1 {
		
		prevLog := rf.log[args.PrevLogIndex]
		if prevLog == nil || prevLog.Term != args.PrevLogTerm{
			
			if prevLog == nil {
				rf.logger.Errorf("Got empty log pntr at rf.log[%d]", args.PrevLogIndex)
			} else {
				rf.logger.Warnf("Local log term and leader log term mismatch (index:%d, Term(%d!=%d))", args.PrevLogIndex, prevLog.Term, args.PrevLogTerm)
			}
			
			reply.Success = false
			return
		}
	} else {
		rf.logger.Warnf("args.PrevLogIndex(%d) larger than local log length(%d), could be heartbeat", args.PrevLogIndex, len(rf.log))
	}
	
	if args.Entries == nil {
		// Step down to follower when receiving heartbeat
		rf.logger.Debugf("Term %d: Got heartbeat from leader(%d)", args.Term, args.LeaderId)
		if rf.state != FOLLOWER {
			rf.logger.Debugf("Convert into follower (Before was: %s)", rf.state)
			rf.state = FOLLOWER
		}
		rf.electionTimerReset <- struct {}{} // refresh electiontimeouttimer
	} else {
		// Check new log entry validity
		nextLogIndex := args.PrevLogIndex + 1
		if nextLogIndex <= len(rf.log)-1 && len(args.Entries) >= 1{
			if rf.log[nextLogIndex].Term != args.Entries[0].Term {
				rf.logger.Debugf("Detect new log entry mismatch at index=%d (Term %d != %d)", nextLogIndex, rf.log[nextLogIndex].Term, args.Entries[0].Term)
				for i := len(rf.log)-1; i >= nextLogIndex; i-- {
					// pop last element K times
					rf.logger.Debugf("Removing rf.log[%d]", i)
					rf.log[len(rf.log)-1] = nil
					rf.log = rf.log[:len(rf.log)-1]
				}
			}
		}
		
		// Append new log entry not already in local log
		rf.logger.WithFields(log.Fields{
			"index": nextLogIndex,
			"term": args.Entries[0].Term,
			"cmd": args.Entries[0].Command,
		}).Debug("Appending new log...")
		rf.log = append(rf.log, &LogEntry{Term: args.Entries[0].Term, Command: args.Entries[0].Command})
	}
	
	// If leaderCommit > commitIndex, set commitIndex =
	//	min(leaderCommit, index of last new entry)
	// Check & advance commitIndex
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		rf.logger.Debugf("Follower updating commitIndex from %d to %d", oldCommitIndex, rf.commitIndex)
		rf.applyMsgInterrupt <- struct{}{}
	}
	
	return
	
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := true

	// Your code here (2B).
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// reject client request if server is not leader
	if rf.state != LEADER {
		return index, term, false // other two replied fields won't matter if isn't leader
	}
	
	// If command received from client: append entry to local log
	// TODO: check if we need to respond after entry applied to state machine (§5.3)
	index = len(rf.log)+1 // off by one...
	term = rf.currentTerm
	rf.log = append(rf.log, &LogEntry{Term: term, Command: command})
	rf.logger.WithFields(log.Fields{
		"term": term,
		"logIndex": index,
		"cmd": command,
	}).Debug("Leader appended new command to log")
	
	//- If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	for peerIndex, _ := range rf.peers {
		if peerIndex == rf.me { continue }
		
		// start a goroutine that sends AE rpc with log cmd from nextIndex up to last log index
		go func(server int) {
			rf.mu.RLock()
			nextIndex := rf.nextIndex[server]
			lastLogIndex := len(rf.log) - 1
			rf.mu.RUnlock()
			
			for nextIndex <= lastLogIndex {
				// send AE rpc
				rf.mu.RLock()
				prevLogTerm := 0
				prevLogIndex := nextIndex - 1 // TODO: check here!!!
				if nextIndex > 0 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: rf.commitIndex,
					Entries:      []LogEntry{*rf.log[nextIndex]}, // send 1 log msg at a time
				}
				rf.mu.RUnlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.logger.Debugf("Peer(%d) term larger than currentTerm (%d > %d)", server, reply.Term, rf.currentTerm)
						rf.logger.Debug("Converting to follower...")
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.voteCount = 0
						rf.votedFor = nil
						rf.electionTimerReset <- struct{}{}
						return
					}
					
					// If successful: update nextIndex and matchIndex for follower (§5.3)
					if reply.Success == true {
						rf.nextIndex[server] += 1
						rf.matchIndex[server] = nextIndex //?
						rf.logger.WithFields(log.Fields{
							"server": server,
							"newMatchIndex": rf.matchIndex[server],
							"newNextIndex": rf.nextIndex[server],
						}).Debug("AE reply success, updating matchIndex and nextIndex")
						
						// decide if commitIndex needs be incremented
						// If there exists an N such that N > commitIndex, a majority
						// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
						// set commitIndex = N (§5.3, §5.4).
						N, hasNewCommitIndex := decideNextCommitIndex(rf)
						if hasNewCommitIndex {
							rf.logger.Debugf("Advancing commitIndex from %d to %d", rf.commitIndex, N)
							rf.commitIndex = N
							rf.applyMsgInterrupt <- struct{}{}
						}
					} else {
					// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
						rf.nextIndex[server] -= 1
						rf.logger.WithFields(log.Fields{
							"server": server,
							"newNextIndex": rf.nextIndex[server],
						}).Debug("AE reply false, decreasing nextIndex")
					}
					
					nextIndex = rf.nextIndex[server]
					lastLogIndex = len(rf.log) - 1
					
					rf.mu.Unlock()
				}
			}
		}(peerIndex)
	}
	return index, term, isLeader
}

// TODO: decide if we need to put this into a independent go routine triggered by sync.Cond?
func decideNextCommitIndex(rf *Raft) (int, bool) {
	ok := false
	N := -1
	sortedMatchIndex := make([]int, 0, len(rf.matchIndex) - 1) // exclude self
	for i, value := range rf.matchIndex {
		if i == rf.me { continue }
		if value <= rf.commitIndex { continue }
		sortedMatchIndex = append(sortedMatchIndex, value)
	}
	if len(sortedMatchIndex) == 0 { return N, ok }
	sort.Sort(sort.Reverse(sort.IntSlice(sortedMatchIndex))) // reverse sort O(PlnP)
	
	minVoteCount := (len(rf.peers) - 1)/2
	for i, value := range sortedMatchIndex { // O(P)
		vote := (i+1)
		if vote >= minVoteCount {
			N = value
			ok = true
			break
		}
	}
	return N, ok
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyMsgCh = applyCh
	rf.lastApplied = -1
	rf.commitIndex = -1
	
	// Your initialization code here (2A, 2B, 2C).
	log.SetLevel(log.InfoLevel)
	//log.SetLevel(log.DebugLevel)
	rf.mu.Lock()
	rf.logger = log.WithFields(log.Fields{
		"Peer": me,
	})
	rf.electionTimeout = time.Duration(550 + (rand.Int63() % 100)) * time.Millisecond // 550~650ms
	rf.heartbeatInterval = time.Duration(50) * time.Millisecond
	rf.state = FOLLOWER
	rf.electionTimerReset = make(chan struct{})
	rf.applyMsgInterrupt = make(chan struct{}, len(rf.peers)-1)
	rf.mu.Unlock()
	
	go startLeaderElectionRoutine(rf) // terminate & cleanup when rf.killed() is true
	
	go sendLeaderHeartbeatRoutine(rf) // terminate & cleanup when rf.killed() is true
	
	go applyLogCommandRoutine(rf) // terminate & cleanup when rf.killed() is true
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	return rf
}

// Check matchIndex periodically and decide if commitIndex needs be updated
// Apply new log msg to state machine accordingly
func applyLogCommandRoutine(rf *Raft) {
	for {
		if rf.killed() {
			rf.logger.Info("Raft server killed. Stopping applyLogCommand thread...")
			return
		}
		
		<-rf.applyMsgInterrupt
		rf.mu.Lock()
		rf.logger.Debug("Got applyMsgInterrupt!!!!")
		for i := rf.lastApplied; i <= rf.commitIndex; i++ {
			if i < 0 { continue }
			newMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i+1,
			}
			rf.logger.WithFields(log.Fields{
				"cmdIndex": i+1,
				//"cmd": rf.log[i].Command,
			}).Infof("Applying new msg to state machine")
			rf.applyMsgCh <- newMsg
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	}
}

// Gorountine that sends periodic heartbeat if current server think it's the leader
func sendLeaderHeartbeatRoutine(rf *Raft) {
	
	for {
		if rf.killed() {
			rf.logger.Info("Raft server killed. Stopping heartbeat thread...")
			return
		}
		
		time.Sleep(rf.heartbeatInterval)
		
		rf.mu.RLock()
		curState := rf.state
		rf.mu.RUnlock()
		
		if curState == LEADER {
			
			// send AppendEntries RPC to all follower peers
			// TODO: check if we need to keep sending AE rpc until follower's commitIndex matches?
			for index, _ := range rf.peers {
				if index == rf.me { continue }
				// send RPC and handle reply in the same goroutine
				
				go func(server int) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					
					prevLogIndex := rf.nextIndex[server] - 1
					prevLogTerm := 0
					if prevLogIndex >= 0 {
						prevLogTerm = rf.log[prevLogIndex].Term
					}
					args := AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm: prevLogTerm,
						LeaderCommit: rf.commitIndex,
						Entries: nil, // Heartbeat rpc won't carry any log entries
					}
					
					rf.logger.WithFields(log.Fields{
						"prevLogIndex": prevLogIndex,
						"prevLogTerm": prevLogTerm,
						"leaderCommitIndex": rf.commitIndex,
					}).Debugf("Sending heartbeat to peer(%d)", server)
					
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)
					
					if ok {
						if reply.Term > rf.currentTerm {
							rf.logger.Debugf("Peer(%d) term larger than currentTerm (%d > %d)", server, reply.Term, rf.currentTerm)
							rf.logger.Debug("Converting to follower...")
							rf.currentTerm = reply.Term
							rf.state = FOLLOWER
							rf.voteCount = 0
							rf.votedFor = nil
							rf.electionTimerReset <- struct{}{}
							return
						}
						// Heartbeat AE rpc reply handler won't change rf.nextIndex (it's Start(cmd)'s job...)
					}
				}(index)
			}
		}
	}
	
	return
}

// Goroutine that triggers leader reelection and calls peer RequestVote RPC whenever electiontimeout timer is up
func startLeaderElectionRoutine(rf *Raft) {
	
	rf.mu.RLock()
	electionTimeout := rf.electionTimeout
	curState := rf.state
	rf.mu.RUnlock()
	
	rf.logger.WithFields(log.Fields{
		"timeout": electionTimeout,
		"state": curState,
	}).Info("Starting leader election routine")
	
	for {
		if rf.killed() {
			rf.logger.Info("Raft server killed. Stopping leader election thread...")
			break
		}
		
		rf.mu.RLock()
		curState = rf.state
		rf.mu.RUnlock()
		
		if curState == LEADER {
			time.Sleep(30 * time.Millisecond)
			continue
		} else { // FOLLOWER & CANDIDATE
			// On each iteration new timer is created
			select {
			case <-time.After(electionTimeout):
				rf.logger.Debug("ElectionTimeout! Calling peers' RequestVoteRPC")
				
				// To begin an election, a follower increments its current
				// term and transitions to candidate state. It then votes for
				// itself and issues RequestVote RPCs in parallel to each of
				// the other servers in the cluster
				
				rf.mu.Lock()
				// increment term no.
				rf.state = CANDIDATE
				rf.currentTerm += 1
				rf.votedFor = &rf.me
				rf.voteCount = 0
				args := RequestVoteArgs{
					Terms: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: 0, // TODO: impl. election restriction
					LastLogTerm: 0, // TODO: impl. election restriction
				}
				rf.mu.Unlock()
				
				// send RequestVoteRPC to all peers (in parallel using goroutines)
				// replies are handled in parallel, voteCount increment if vote granted; change state if got majority vote
				for index, _ := range rf.peers {
					if index == rf.me { continue }
					go func(server int) {
						reply := RequestVoteReply{}
						rf.logger.WithFields(log.Fields{
							"server": server,
						}).Debugf("Sending requestvote RPC from candidate(%d)", args.CandidateId)
						rf.sendRequestVote(server, &args, &reply)
						
					}(index)
				}
				
				
			case <-rf.electionTimerReset: // Got timeout refresh interrupt from other events
				rf.logger.Debug("Got reset Timer interrupt!")
			}
		}
	}

}
