package deadpool

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Task interface {
	Run()
}

type Executor interface {
	Execute(Task, Pool)
}

type WorkerSpawner interface {
	Spawn(chan Task, chan struct{}, func(Task), func(), func())
}

type Pool interface {
	Submit(Task)
	WaitAll()
	Execute(Task)
	SpawnWorker()

	WorkerSpawned() int32
	SubmittedTasks() int32

	OnExecuteTask(time.Time)

	CummlatedWorkTime() time.Duration
	AVGTime() time.Duration

	StopWorkers()
}

type DefaultExecutor struct{}

func (e *DefaultExecutor) Execute(task Task, p Pool) {
	start := time.Now().UTC()
	defer p.OnExecuteTask(start)
	task.Run()
}

type DefaultSpawner struct{}

func (s *DefaultSpawner) Spawn(tasks chan Task, stopWorkers chan struct{}, executor func(Task), onStart func(), onEnd func()) {
	onStart()

	go func() {
		defer onEnd()

		for {
			select {
			case task := <-tasks:
				if task == nil {
					return
				}

				executor(task)
			case <-stopWorkers:
				return
			}
		}

	}()
}

type deadpool struct {
	executor          Executor
	spawner           WorkerSpawner
	recoverer         func()
	wg                *sync.WaitGroup
	stopWorkers       chan struct{}
	tasksStream       chan Task
	avgTaskTime       time.Duration
	cummulatedTime    time.Duration
	mu                sync.Mutex
	maxWorkers        int32
	submittedTasksQTY int32
	readyTasksQTY     int32
	currentWorkersQTY int32
	cap               int8
}

func New(opts ...func(*deadpool)) (*deadpool, error) {
	d := &deadpool{
		executor: &DefaultExecutor{},
		spawner:  &DefaultSpawner{},
		wg:       &sync.WaitGroup{},
		mu:       sync.Mutex{},
	}

	for _, f := range opts {
		f(d)
	}

	if d.maxWorkers <= 0 {
		return d, ErrInvalidMaxWorkers
	}

	if d.cap < 0 {
		return d, ErrInvalidCap
	}

	if d.executor == nil {
		return d, ErrInvalidExecutor
	}

	if d.spawner == nil {
		return d, ErrInvalidSpawner
	}

	d.tasksStream = make(chan Task, d.cap)
	d.stopWorkers = make(chan struct{})

	return d, nil
}

func (d *deadpool) Submit(task Task) {
	atomic.AddInt32(&d.readyTasksQTY, 1)
	d.SpawnWorker()
	d.tasksStream <- task

	atomic.AddInt32(&d.submittedTasksQTY, 1)
}

func (d *deadpool) StopWorkers() {
	close(d.stopWorkers)
}

func (d *deadpool) WaitAll() {
	if d.tasksStream != nil {
		close(d.tasksStream)
	}

	d.wg.Wait()
	d.StopWorkers()
}

func (d *deadpool) Execute(task Task) {
	d.executor.Execute(task, d)
}

func (d *deadpool) SpawnWorker() {
	currentWorkersQTY := atomic.LoadInt32(&d.currentWorkersQTY)

	if currentWorkersQTY < atomic.LoadInt32(&d.maxWorkers) {

		d.spawner.Spawn(
			d.tasksStream,
			d.stopWorkers,
			d.Execute,
			func(workerID int32) func() {
				return func() {
					d.wg.Add(1)
				}
			}(currentWorkersQTY),
			func(workerID int32) func() {
				return func() {
					d.wg.Done()
				}
			}(currentWorkersQTY),
		)

		atomic.AddInt32(&d.currentWorkersQTY, 1)
	}
}

func (d *deadpool) WorkerSpawned() int32 {
	return d.currentWorkersQTY
}

func (d *deadpool) SubmittedTasks() int32 {
	return d.submittedTasksQTY
}

func (d *deadpool) AVGTime() time.Duration {
	return d.avgTaskTime
}

func (d *deadpool) CummlatedWorkTime() time.Duration {
	return d.cummulatedTime
}

func (d *deadpool) OnExecuteTask(start time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	ellapsed := time.Since(start)
	d.cummulatedTime += ellapsed
	readies := time.Duration(atomic.LoadInt32(&d.readyTasksQTY))
	d.avgTaskTime = d.cummulatedTime / readies
}

func WithMax(max int32) func(*deadpool) {
	return func(d *deadpool) {
		d.maxWorkers = max
	}
}

func WithCap(cap int8) func(*deadpool) {
	return func(d *deadpool) {
		d.cap = cap
	}
}

func WithExecutor(exe Executor) func(*deadpool) {
	return func(d *deadpool) {
		d.executor = exe
	}
}

func WithSpawner(spn WorkerSpawner) func(*deadpool) {
	return func(d *deadpool) {
		d.spawner = spn
	}
}

func WithRecoverer(f func()) func(*deadpool) {
	return func(d *deadpool) {
		if f != nil {
			d.recoverer = f
			return
		}
		d.recoverer = func() {}
	}
}

var (
	ErrInvalidMaxWorkers = errors.New("invalid max number of workers")
	ErrInvalidCap        = errors.New("invalid capacity for tasks stream")
	ErrInvalidExecutor   = errors.New("expected a valid Executor. Nil received")
	ErrInvalidSpawner    = errors.New("expected a valid Spawner. Nil received")
)
