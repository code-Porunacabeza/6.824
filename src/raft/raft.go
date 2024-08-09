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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Status int

type VoteState int

type AppendEntriesState int

var HeartBeatTimeout = 120 * time.Millisecond //全局心跳超时时间

const (
	Follower  Status = iota //跟随者
	Candidate               //竞选者
	Leader                  //领导者
)

const (
	Normal VoteState = iota //投票过程正常
	Killed                  //raft节点已经终止
	Expire                  //投票过期
	Voted                   //本任期term内已经投过票
)

const (
	AppNormal    AppendEntriesState = iota //追加正常
	AppOutOfDate                           //追加过时
	AppKilled                              //raft程序终止
	//AppRepeat                              //追加重复
	AppCommited //追加的日志已经提交
	Mismatch    //追加不匹配
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 邻居节点通过RPC连接发送消息
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//所有servers拥有的变量
	currentTerm int        //当前任期
	votedFor    int        //把票投给了谁
	logs        []LogEntry //日志条目数组，状态机要执行的指令，还有收到的任期号

	commitIndex int //状态机中已知的被提交的日志条目的索引值
	lastApplied int //最后一个被追加到状态机日志的索引值

	nextIndex  []int //需要发送的下一个日志条目的索引值，对应每个raft节点，从哪个位置待追加到每个raft节点
	matchIndex []int //已经赋值的最后日志条目下标

	status   Status        //该节点是什么状态
	overtime time.Duration //超时时间， 200-400ms
	timer    *time.Ticker  //每个节点计时器，用于选举leader

	applyChan chan ApplyMsg //日志存在这里，client去取，返回给应用层
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int        //任期
	LeaderId     int        //leader自身的ID
	PrevLogIndex int        //预计要从哪里追加log的index
	PrevLogTerm  int        //追加新的日志的任期号
	Entries      []LogEntry //预计存储的日志
	LeaderCommit int        //leader的commit index
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	AppState    AppendEntriesState //追加状态
	UpNextIndex int                //更新请求节点的nextIndex[i]
}

// return currentTerm and whether this server
// believes it is the leader.
// 获取raft节点状态
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //需要竞选的人的任期
	CandidateId  int //需要竞选的人的ID
	LastLogIndex int //竞选人日志条目的最后索引
	LastLogTerm  int //候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       //投票方的Term，如果竞选者比自己还低就改成这个
	VoteGranted bool      //是否投票给了该竞选人
	VoteState   VoteState //投票状态
}

// example RequestVote RPC handler.
// 主要处理别的节点的票数请求的消息，相当于replyRequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	fmt.Printf("receive server term %v, current node term %v \n", args.Term, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.votedFor == -1 {
		currentLogIndex := len(rf.logs) - 1
		currentLogTerm := 0

		if currentLogIndex >= 0 {
			currentLogTerm = rf.logs[currentLogIndex].Term
		}
		fmt.Printf("receive server LastLogterm %v, current Log term %v , LastLogIndex %v, len of current logs %v\n ", args.LastLogTerm, currentLogTerm, args.LastLogIndex, len(rf.logs))
		if args.LastLogTerm < currentLogTerm || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)-1) {
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.timer.Reset(rf.overtime)
	} else {
		reply.VoteState = Voted
		reply.VoteGranted = false

		if rf.votedFor != args.CandidateId {
			return
		} else {
			rf.status = Follower
		}
		rf.timer.Reset(rf.overtime)
	}
	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) //发送给另一个Raft节点，请求票数, 收到的reply存储另一个节点的投票信息，通过RPC调用另一个节点的方法，这个方法就是回复投票信息的

	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("[sendRequestVote(%v) ] term %v: send a election to %v\n", rf.me, rf.currentTerm, server)
	// 如果请求投票的节点term比自己的还小，不投票
	fmt.Printf("ReplyRequestVote(%v) term %v\n", reply.VoteState, reply.Term)
	if reply.Term < rf.currentTerm {
		return false //任期错误
	}
	switch reply.VoteState {
	case Expire: //过期了，说明已经有人选上了leader
		{
			rf.status = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Normal, Voted:
		{
			if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
				*voteNums++
			}
			if *voteNums >= (len(rf.peers)/2)+1 { //如果当前的票数多于总节点数的一半以上
				*voteNums = 0
				if rf.status == Leader { //这个节点被选为leader
					return ok
				}
				rf.status = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				for i, _ := range rf.nextIndex { //设置nextIndex,leader来同步log
					rf.nextIndex[i] = len(rf.logs) + 1
				}
				rf.timer.Reset(HeartBeatTimeout) //重置心跳发送函数
				fmt.Printf("[	sendRequestVote-func-rf(%v)		] be a leader\n", rf.me)
			}
		}
	case Killed:
		return false
	}
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是leader, 直接返回
	if rf.status != Leader {
		return index, term, false
	}
	//初始化日志条目，追加
	appendLog := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs)
	term = rf.currentTerm
	return index, term, isLeader
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) bool {

	if rf.killed() {
		return false
	}

	// paper中5.3节第一段末尾提到，如果append失败应该不断的retries ,直到这个log成功的被store
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {

		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	}

	// 必须在加在这里否则加载前面retry时进入时，RPC也需要一个锁，但是又获取不到，因为锁已经被加上了
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对reply的返回状态进行分支
	switch reply.AppState {

	// 目标节点crash
	case AppKilled:
		{
			return false
		}

	// 目标节点正常返回
	case AppNormal:
		{
			// 2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好
			// 判断是否达到半数服务器的同意
			if reply.Success && reply.Term == rf.currentTerm && *appendNums <= len(rf.peers)/2 {
				*appendNums++
			}
			// 要追加的raft节点的nextIndex大于本节点本身的，所以不能添加，需要撤回一部分
			if rf.nextIndex[server] > len(rf.logs)+1 {
				return false
			}
			rf.nextIndex[server] += len(args.Entries)
			if *appendNums > len(rf.peers)/2 { // 达到半数以上的需求
				*appendNums = 0
				if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
					return false
				}
				for rf.lastApplied < len(rf.logs) {
					rf.lastApplied++
					applyMeg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.lastApplied-1].Command,
						CommandIndex: rf.lastApplied,
					}
					rf.applyChan <- applyMeg //向应用层返回
					rf.commitIndex = rf.lastApplied
					fmt.Printf("[sendAppendEntries func-rf(%v)] commitLog \n", rf.me)
				}
			}
			fmt.Printf("[sendAppendEntries func-rf(%v)] rf.log: %+v ; rf.lastApplied: %v\n", rf.me, rf.logs, rf.lastApplied)
			return true
		}
	case Mismatch:
		{
			if args.Term != rf.currentTerm {
				return false
			}
			rf.nextIndex[server] = reply.UpNextIndex
		}

	//If AppendEntries RPC received from new leader: convert to follower(paper - 5.2)
	//reason: 出现网络分区，该Leader已经OutOfDate(过时）
	case AppOutOfDate:
		{
			// 该节点变成追随者,并重置rf状态
			rf.status = Follower
			rf.votedFor = -1
			rf.timer.Reset(rf.overtime)
			rf.currentTerm = reply.Term
		}

	case AppCommited:
		{
			if args.Term != rf.currentTerm {
				return false
			}
			rf.nextIndex[server] = reply.UpNextIndex
		}

	}

	return ok
}

// AppendEntries 建立心跳、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 节点crash
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}

	// 出现网络分区，args的任期，比当前raft的任期还小，说明args之前所在的分区已经OutOfDate
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//出现冲突，新的leader与follower的Log不一致
	// 1.如果preLogIndex大于当前日志的下标，说明follower缺失日志，拒绝添加，回复消息，进行日志恢复与leader保持一致
	// 2.如果preLog的任期号与preLogIndex处的任期不相等，说明日志中存在冲突，拒绝添加
	if args.PrevLogIndex > 0 && (len(rf.logs) < args.PrevLogIndex || rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}

	// 如果当前节点提交的index比传过来的还高，说明当前节点的日志已经超前，返回
	if args.PrevLogIndex != -1 && rf.lastApplied > args.PrevLogIndex {
		reply.AppState = AppCommited
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}
	// 对当前的rf进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.overtime)

	// 对返回的reply进行赋值
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	// 如果存在日志包，那么追加
	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
	}

	// 将日志提交至与leader相同
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied-1].Command,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
		fmt.Printf("[AppendEntries func-rf(%v)] commit Log \n", rf.me)
	}
	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// 选举定时器
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//当定时器结束进行超时选举
		select {
		case <-rf.timer.C: //rf中的timer每隔一段时间就会通过通道发送触发消息，这个时间间隔是超时消息，说明leader丢失了
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			//根据自身的状态发送下一条消息
			switch rf.status {
			case Follower:
				rf.status = Candidate
				fallthrough //这个关键字的作用就是执行下一个case,不管下一个case满不满足，如果发现leader丢失了，当前又是follower，那么直接将身份转为Candidate，并执行候选者的代码
			case Candidate:
				rf.currentTerm += 1 //任期数+1
				rf.votedFor = rf.me //给自己投一票
				votedNums := 1      //自身票数

				//每轮选举开始时，重新设置选举超时
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)

				//对自身以外的节点进行选举
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}
					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
				}
			case Leader: //如果是领导者，那就发送添加Log的请求
				appendNums := 1
				// 进行心跳/日志同步
				rf.timer.Reset(HeartBeatTimeout)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					appendEntriesReply := AppendEntriesReply{}
					// 如果nextIndex[i]的长度不等于rf.logs,代表leader的log entries不一致，需要附带过去
					appendEntriesArgs.Entries = rf.logs[rf.nextIndex[i]-1:] //初始状态下，要发送的entry就是next数组之后的所有log
					// 如果初始值不是0
					if rf.nextIndex[i] > 0 {
						// 默认PreLogIndex就是next数组前面的一个节点，leader要发给每个raft区前探
						appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
					}
					if appendEntriesArgs.PrevLogIndex > 0 {
						fmt.Println("Server:", rf.me, "toServer", i, "len(rf.logs):", len(rf.logs), "prevLogIndex:", appendEntriesArgs.PrevLogIndex, "rf.nextIndex[i]:", rf.nextIndex[i])
						appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex-1].Term
					}
					go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)
				}
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//初始化
	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overtime = time.Duration(150+rand.Intn(100)) * time.Millisecond //随机产生150-350ms，//设置的是超时时间
	rf.timer = time.NewTicker(rf.overtime)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// 开启定时选举周期
	go rf.ticker()

	return rf
}
