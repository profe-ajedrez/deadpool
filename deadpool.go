// Package deadpool. Dead simple worker pool. Pun intended.
//
// # Modo de uso
//
// 1 Defina sus tareas o trabajos implementando la interface [Task]
//
// 2 Construya un nuevo deadpool
//
// 3 Agregue sus tareas
//
// 4 Ejecute
package deadpool

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Pool se ofrece como interface que expone la funcionalidad de Deadpool
type Pool interface {
	Start()
	Stop()
	AddWork(Task)

	WorkerNumber() uint8

	MaxExeTime() time.Duration
	MinExeTime() time.Duration
	AvgExetime() time.Duration
	ExecutedTasks() int
}

// Task expone la interface que los trabajos tendrán que implementar
type Task interface {
	Run() error
	OnFailure(error)
	ExeTime() time.Duration
}

// Deadpool es el worker pool ofrecido por este paquete. Y si, su nombre es por el personaje de Marvel Duh!
//
// # Ejemplo
//
//	type testTask struct {
//		executeFunc func() error
//
//		shouldErr bool
//		wg        *sync.WaitGroup
//
//		mFailure       *sync.Mutex
//		failureHandled bool
//
//		start    time.Time
//		ellapsed time.Duration
//	}
//
//	func newTestTask(executeFunc func() error, shouldErr bool, wg *sync.WaitGroup) *testTask {
//		return &testTask{
//			executeFunc: executeFunc,
//			shouldErr:   shouldErr,
//			wg:          wg,
//			mFailure:    &sync.Mutex{},
//		}
//	}
//
//	func (t *testTask) Run() error {
//		if t.wg != nil {
//			defer t.wg.Done()
//		}
//
//		defer func() { t.ellapsed = time.Since(t.start) }()
//
//		if t.executeFunc != nil {
//			t.start = time.Now().UTC()
//			return t.executeFunc()
//		}
//
//		// if no function provided, just wait and error if told to do so
//		time.Sleep(50 * time.Millisecond)
//		if t.shouldErr {
//			return fmt.Errorf("planned Execute() error")
//		}
//
//		return nil
//	}
//
//	func (t *testTask) OnFailure(e error) {
//		t.mFailure.Lock()
//		defer t.mFailure.Unlock()
//
//		t.failureHandled = true
//	}
//
//	func (t *testTask) hitFailureCase() bool {
//		t.mFailure.Lock()
//		defer t.mFailure.Unlock()
//
//		return t.failureHandled
//	}
//
//
//	func (t *testTask) ExeTime() time.Duration {
//		return t.ellapsed
//	}
//
//	var tasks []*testTask
//
//	wg := &sync.WaitGroup{}
//
//	for i := 0; i < 20; i++ {
//		wg.Add(1)
//		tasks = append(tasks, newTestTask(
//			func() error { time.Sleep(time.Nanosecond * 1); return nil },
//			false,
//			wg,
//			),
//		)
//	}
//
//	p := NewDeadpool(5, uint8(len(tasks)), loggerForTest{})
//
//	p.Start()
//
//	for _, j := range tasks {
//		p.AddWork(j)
//	}
//
//	wg.Wait()
//
//	for taskNum, task := range tasks {
//		if task.hitFailureCase() {
//			t.Fatalf("error function called on task %d when it shouldn't be", taskNum)
//		}
//			fmt.Printf("dur: %v\n", task.ExeTime())
//		}
//	}
//
//	fmt.Printf("avg: %v\n", p.AvgExetime())
//	fmt.Printf("min: %v\n", p.MinExeTime())
//	fmt.Printf("max: %v\n", p.MaxExeTime())
type Deadpool struct {
	numWks uint8
	tasks  chan Task

	start sync.Once

	stop sync.Once

	stopAll chan struct{}
	logger  Logger

	acumTime      time.Duration
	executedTasks int

	maxExetime time.Duration
	minExetime time.Duration
}

func NewDeadpool(numWk uint8, size uint8, logger Logger) Pool {
	tasks := make(chan Task, size)

	if logger == nil {
		logger = emptylogger{}
	}

	return &Deadpool{
		numWks:   numWk,
		tasks:    tasks,
		start:    sync.Once{},
		stop:     sync.Once{},
		stopAll:  make(chan struct{}),
		logger:   logger,
		acumTime: 0 * time.Microsecond,
	}
}

func (p *Deadpool) Start() {
	p.start.Do(func() {
		log.Print("starting simple worker pool")
		p.startWorkers()
	})
}

func (p *Deadpool) Stop() {
	p.stop.Do(func() {
		log.Print("stopping simple worker pool")
		close(p.stopAll)
	})
}

func (p *Deadpool) AddWork(t Task) {
	select {
	case p.tasks <- t:
	case <-p.stopAll:
	}
}

func (p *Deadpool) startWorkers() {
	for i := 0; i < int(p.numWks); i++ {
		go func(workerNum int) {
			p.logger.Info(fmt.Sprintf("starting worker %d", workerNum))

			for {
				select {
				case <-p.stopAll:
					p.logger.Info(fmt.Sprintf("stopping worker %d with quit channel\n", workerNum))
					return
				case task, ok := <-p.tasks:
					if !ok {
						p.logger.Info(fmt.Sprintf("stopping worker %d with closed tasks channel\n", workerNum))
						return
					}

					if err := task.Run(); err != nil {
						task.OnFailure(err)
					}

					p.executedTasks += 1
					exeTime := task.ExeTime()
					p.acumTime += exeTime

					if exeTime > p.maxExetime {
						p.maxExetime = exeTime
					}

					if exeTime < p.minExetime {
						p.minExetime = exeTime
					}
				}
			}
		}(i)
	}
}

// ExecutedTasks implements Pool.
func (p *Deadpool) ExecutedTasks() int {
	return p.executedTasks
}

// AvgExetime implements Pool.
func (p *Deadpool) AvgExetime() time.Duration {
	return p.acumTime / time.Duration(p.executedTasks)
}

// MaxExeTime implements Pool.
func (p *Deadpool) MaxExeTime() time.Duration {
	return p.maxExetime
}

// MinExeTime implements Pool.
func (p *Deadpool) MinExeTime() time.Duration {
	return p.minExetime
}

// WorkerNumber implements Pool.
func (p *Deadpool) WorkerNumber() uint8 {
	return p.numWks
}

type Logger interface {
	Log(message interface{})
	Panic(message interface{})
	Fatal(message interface{})
	Error(message interface{})
	Warn(message interface{})
	Info(message interface{})
	Debug(message interface{})
	Trace(message interface{})
}

type emptylogger struct{}

func (e emptylogger) Log(message interface{})   {}
func (e emptylogger) Panic(message interface{}) {}
func (e emptylogger) Fatal(message interface{}) {}
func (e emptylogger) Error(message interface{}) {}
func (e emptylogger) Warn(message interface{})  {}
func (e emptylogger) Info(message interface{})  {}
func (e emptylogger) Debug(message interface{}) {}
func (e emptylogger) Trace(message interface{}) {}
