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
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const (
	Follower = 0
	Candidate = 1
	Leader = 2
	HBeatInterval = 100

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

type LogEntry struct {
	// LogIndex   int
	LogTerm    int
	LogCommand interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm 	int
	votedFor		int
	log				[]LogEntry

	// Volatile state on all servers:
	commitIndex		int
	lastApplied		int

	// Volatile state on leaders:
	nextIndex		[]int
	matchIndex		[]int

	state 			int
	votecount		int

	appendEntries	chan bool
	heartBeat		chan bool		// 收到心跳信号
	applyCh			chan ApplyMsg	//
	voteGranted		chan bool		// 是否已经投过票
	isLeader		chan bool		// 是否是leader
	commitCh		chan bool

	heartbeatTimeout   	time.Duration
	electionTimeout		time.Duration

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}
/*
func (rf *Raft) GetLastIndex() int {
	return len(rf.log)-1
}

func (rf *Raft) GetLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}
*/

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
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	d := gob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateID		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

type AppendEntriesArgs struct {
	Term			int
	LeaderID		int
	PrevLogIndex	int
	PrevLogTerm		int
	LeaderCommit	int
	Entries			[]LogEntry
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
	NextIndex		int
	ConflictTerm	int
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.mu.Lock()
	rf.currentTerm += 1
	//rf.mu.Unlock()
	rf.votedFor = rf.me
	rf.votecount = 1
	rf.persist()
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[len(rf.log) - 1].LogTerm
	rf.mu.Unlock()

	for i:=0; i<len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int, args RequestVoteArgs) {
			reply := &RequestVoteReply{}
			num := len(rf.peers)/2 + 1
			ok := rf.sendRequestVote(index, &args, reply)
			if !ok {
				DPrintf("send RequestVote fail, Term %d, CandidateID %d", args.Term, args.CandidateID)
				return
			}
			if reply.VoteGranted == false {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
				}
				rf.mu.Unlock()
			}else {
				rf.mu.Lock()
				rf.votecount++
				if rf.votecount >= num && rf.state == Candidate {
					DPrintf("%d wins election", rf.me)
					rf.state = Leader
					rf.isLeader <- true
				}
				rf.mu.Unlock()
			}

		}(i, args)
	}

}

//
// example RequestVote RPC handler.
// receiver
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	// 如果当前server term大于候选人term 不投票 或者已经投给其他人
	if (rf.currentTerm > args.Term) || (rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 假如有过时的领导人 或者投票没成功也应该更新term 2B
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// 2B
	lastTerm := rf.log[len(rf.log) - 1].LogTerm
	if (args.LastLogTerm < lastTerm) || ((args.LastLogTerm == lastTerm) && (args.LastLogIndex < len(rf.log) - 1)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateID
	rf.persist()
	//rf.currentTerm = args.Term  // 为何要去掉
	reply.VoteGranted = true
	rf.voteGranted <- true
	//rf.state = Follower
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
	return ok
}


func (rf *Raft) broadcastAppendEntries() {
	for i:=0; i<len(rf.peers); i++ {
		//time.Sleep(10 * time.Millisecond)
		if i == rf.me{
			continue
		}
		go func(index int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[index] > len(rf.log) {
					return
				}
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				prevLogIndex := rf.nextIndex[index] - 1
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = rf.log[prevLogIndex].LogTerm
				args.LeaderCommit = rf.commitIndex
				entries := make([]LogEntry, 0)
				args.Entries = append(entries, rf.log[rf.nextIndex[index]:]...)
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(index, &args, &reply)
				if !ok {
					//DPrintf("send AppendEntries fail, Term %d, LeadID %d", rf.currentTerm, rf.me)
					return
				}
				rf.mu.Lock()
				if rf.state != Leader || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				if reply.Success == true {
					rf.mu.Lock()
					rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[index] = rf.matchIndex[index] + 1
					//rf.mu.Unlock()
					// 一旦多数节点复制 则领导人提交 并且更新commitIndex
					for i := len(rf.log)-1; i > rf.commitIndex; i-- {
						count := 1
						for j := range rf.peers {
							if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm{
								count++
							}
						}
						if count > len(rf.peers)/2 && rf.state == Leader{
							rf.commitIndex = i
							rf.commitCh <- true
							break
						}
					}
					rf.mu.Unlock()
					return
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.mu.Unlock()
						return
					} else {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[index] = reply.NextIndex
						} else {
							conflictIndex := reply.NextIndex
							if conflictIndex < len(rf.log){
							if rf.log[conflictIndex].LogTerm == reply.ConflictTerm {
								for i := conflictIndex+1; i < rf.nextIndex[index]; i++ {
									if rf.log[i].LogTerm != reply.ConflictTerm {
										break
									}
									conflictIndex += 1
								}
								rf.nextIndex[index] = conflictIndex + 1
							} else if rf.log[reply.NextIndex].LogTerm != reply.ConflictTerm { // 在冲突的index, term > reply.ConflicTerm
								for i := conflictIndex; i > 0; i-- { // 往前寻找，直到term == reply.ConflictTerm
									if rf.log[i].LogTerm == reply.ConflictTerm {
										break
									}
									conflictIndex -= 1
								}
								if conflictIndex == 0 { // 如果日志没有reply.ConflictTerm
									rf.nextIndex[index] = reply.NextIndex
								} else {
									rf.nextIndex[index] = conflictIndex + 1
								}
							} else { // conflictTerm < reply.ConflictTerm，并且必须往前搜索，所以一定找不到任期相等的entry
								rf.nextIndex[index] = reply.NextIndex
							}
						}
						}
					}
					rf.mu.Unlock()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}
}

// receiver
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//defer rf.persist()
	reply.Success = false
	reply.ConflictTerm = -1
	reply.NextIndex = 0
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//reply.NextIndex = len(rf.log)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	// 2B
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	rf.mu.Unlock()
	rf.heartBeat <- true
	reply.Term = args.Term


	//2B
	// 少了几条日志
	rf.mu.Lock()
	if args.Term == rf.currentTerm {
		rf.state = Follower
		if args.PrevLogIndex > len(rf.log) - 1 {
			reply.NextIndex = len(rf.log)
			reply.ConflictTerm = -1
			rf.mu.Unlock()
			return
		}else { // prevlogindex处的日志的任期号不同
			// trick 减少重新发送的次数 一个term一个term的去找
			//rf.mu.Lock()
			if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
				/*reply.ConflictTerm = rf.log[args.PrevLogIndex].LogTerm
				for i:=1; i<len(rf.log); i++ {
					if rf.log[i].LogTerm == rf.log[args.PrevLogIndex].LogTerm {
						reply.NextIndex = i
						break
					}
				}*/

				reply.ConflictTerm = rf.log[args.PrevLogIndex].LogTerm
				reply.NextIndex = args.PrevLogIndex
				for i := reply.NextIndex - 1; i >= 0; i-- {
					if rf.log[i].LogTerm != reply.ConflictTerm {
						break
					} else {
						reply.NextIndex -= 1
					}
				}
				rf.mu.Unlock()
				return
			}
			// 开始复制
			if args.PrevLogIndex == 0 || (args.PrevLogTerm == rf.log[args.PrevLogIndex].LogTerm) {
				reply.Success = true
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				/*index := args.PrevLogIndex
				for i := 0; i < len(args.Entries); i++ {
					index += 1
					if index > len(rf.log)-1 {
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}

					if rf.log[index].LogTerm != args.Entries[i].LogTerm {
						entries := make([]LogEntry, 0)
						copy(entries, args.Entries)
						rf.log = append(rf.log[:index], entries...)
					}
				}*/

				rf.persist()
				//reply.NextIndex = len(rf.log)
				//reply.ConflictTerm = -1
				// 说明leader已经将这些日志commit了 rf.commitIndex是复制内容的前一个
				lastIndex := len(rf.log) - 1
				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit < lastIndex {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = lastIndex
					}
					//rf.sendApplyMsg()
					rf.commitCh <- true
				}
			}
		}
	}

	rf.mu.Unlock()
	// 2A
	//reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
/*
func (rf *Raft) sendApplyMsg() {

	if rf.commitIndex > rf.lastApplied {
		go func(applyIndex int, applyEntries []LogEntry) {
			//rf.mu.Lock()
			for index, entry := range applyEntries {
				var message ApplyMsg
				message.Command = entry.LogCommand
				message.CommandIndex = applyIndex + 1 + index
				message.CommandValid = true
				rf.applyCh <- message
				rf.mu.Lock()
				rf.lastApplied = applyIndex + 1 + index
				rf.mu.Unlock()
			}
			//rf.mu.Unlock()
		}(rf.lastApplied, rf.log[rf.lastApplied+1 : rf.commitIndex+1])
	}
}*/



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
	// 客户机向领导人发送entry
	//term, isLeader = rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogCommand: command})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	//rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{0, 0})
	//rf.commitIndex = 0
	//rf.lastApplied = 0

	rf.heartBeat = make(chan bool, 10000)
	rf.voteGranted = make(chan bool, 10000)
	rf.isLeader = make(chan bool, 10000)
	rf.commitCh = make(chan bool, 10000)
	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = Follower

	rf.heartbeatTimeout = time.Duration(50) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(){
		for {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			electionTimeout := HBeatInterval * 2 + rand.Intn(HBeatInterval)
			rf.mu.Lock()
			rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
			rf.mu.Unlock()

			switch state{
			case Follower:
				select {
				case <- rf.heartBeat:
				case <- rf.voteGranted:
				case <- time.After(rf.electionTimeout):
					rf.mu.Lock()
					rf.state = Candidate
					rf.mu.Unlock()
				}
			case Candidate:
				rf.leaderElection()
				select {
				case <- rf.heartBeat:
				case <- time.After(rf.electionTimeout):
					rf.mu.Lock()
					rf.state = Candidate
					rf.mu.Unlock()
				case <- rf.isLeader:
				}
			case Leader:
				rf.mu.Lock()
				for i := range rf.peers {
					rf.nextIndex[i] = len(rf.log)
				}
				for i := range rf.peers {
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
				rf.broadcastAppendEntries()
				time.Sleep(rf.heartbeatTimeout)
			}
		}
	}()

	go func() {
		for {
			select {
			case <- rf.commitCh:
				rf.mu.Lock()
				if rf.commitIndex > rf.lastApplied {
					applyEntries := rf.log[rf.lastApplied+1 : rf.commitIndex+1]
					applyIndex := rf.lastApplied
					for index, entry := range applyEntries {
						var message ApplyMsg
						message.Command = entry.LogCommand
						message.CommandIndex = applyIndex + 1 + index
						message.CommandValid = true
						rf.applyCh <- message
						rf.lastApplied = applyIndex + 1 + index
					}
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
