package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

const (
	MaxTaskRunTime   = time.Second * 10       // 一个任务的最长执行时间
	ScheduleInterval = time.Millisecond * 500 // 调度间隔
)

type TaskStat struct {
	Status    int       // 任务状态
	WorkerId  int       // 任务被哪个worker执行
	StartTime time.Time // worker执行该任务的起始时间
}

type Master struct {
	phase     TaskPhase  // 当前任务阶段
	taskStats []TaskStat // 任务状态集合
	files     []string   // map task
	nReduce   int        // reduce task 数量
	mu        sync.Mutex // 互斥锁
	done      bool       // reduce任务全部完成
	workerSeq int        // 当前worker总数
	taskCh    chan Task  // 多个rpc共享 都从该通道里取任务
}

//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//reply.Y = args.X + 1
//	return nil
//}

func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock() // workerSeq is shared data, so we should lock it
	defer m.mu.Unlock()
	m.workerSeq++
	reply.WorkerId = m.workerSeq
	return nil
}

// 第seq个任务: MapPhase->第seq个pg-*.txt文件 ReducePhase->mr-*-seq.txt
func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.files),
		Seq:      taskSeq,
		Phase:    m.phase,
		Alive:    true,
	}
	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", m, taskSeq, len(m.files), len(m.taskStats))
	// MapPhase才需要传入文件名 ReducePhase根据NMaps和Seq得到文件名
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

// 维护taskStats
func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}
	allFinish := true
	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady: // 任务还未初始化加入到通道里
			allFinish = false
			m.taskCh <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue: // 任务等待worker来取
			allFinish = false
		case TaskStatusRunning: // 任务正在被worker执行
			allFinish = false
			// 任务超时未完成 将该任务重新初始化并添加到通道里
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr: // 任务出错同样重新初始化
			allFinish = false
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		default:
			panic("t.status err")
		}
	}
	if allFinish {
		if m.phase == MapPhase { // 进入Reduce阶段
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

// 默认MapPhase阶段
func (m *Master) initMapTask() {
	m.phase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
}

// 进入ReducePhase阶段
func (m *Master) initReduceTask() {
	DPrintf("init ReduceTask")
	m.phase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

// 修改task任务状态
func (m *Master) regTask(args *TaskArgs, task *Task) {
	// 加锁
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.phase {
		panic("req Task phase neq")
	}
	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}

// 给worker分配任务
func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	// 从通道里取任务
	task := <-m.taskCh
	reply.Task = &task
	if task.Alive {
		m.regTask(args, &task) // 修改任务状态
	}
	DPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

// 接受worker完成或错误信号 无需相应
func (m *Master) ReportTask(args *ReportArgs, reply *ReportReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	DPrintf("get report task: %+v, taskPhase: %+v", args, m.phase)

	// workerId的作用:由于超时再分配等原因 同一时刻可能有多个worker执行相同任务 但taskStats只记录了当前认可的workerId
	// 因此其他worker,例如超时完成该任务的worker,即使报告结果 也是不被master接受的
	if m.phase != args.Phase || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done { // 任务完成
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else { // 任务失败
		m.taskStats[args.Seq].Status = TaskStatusErr
	}

	go m.schedule()
	return nil
}

// 间隔持续调度
func (m *Master) tickSchedule() {
	// 按说应该是每个 task 一个 timer，此处简单处理
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	// Golang 没有三目运算吗
	// 缓冲区大小为max('M','R')的通道  M R in the paper
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}

	m.initMapTask()
	// 并发执行
	go m.tickSchedule()

	m.server()
	DPrintf("master init")
	return &m
}
