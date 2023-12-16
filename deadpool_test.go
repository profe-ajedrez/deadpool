package deadpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDeadpool_NewDeadpool(t *testing.T) {
	_ = NewDeadpool(0, 0, nil)
}

func TestDeadpool_MultipleStartDontPanic(t *testing.T) {
	p := NewDeadpool(5, 0, nil)

	p.Start()
	p.Start()

	p.Stop()
	p.Stop()
}

type testTask struct {
	executeFunc func() error

	shouldErr bool
	wg        *sync.WaitGroup

	mFailure       *sync.Mutex
	failureHandled bool

	start    time.Time
	ellapsed time.Duration
}

func newTestTask(executeFunc func() error, shouldErr bool, wg *sync.WaitGroup) *testTask {
	return &testTask{
		executeFunc: executeFunc,
		shouldErr:   shouldErr,
		wg:          wg,
		mFailure:    &sync.Mutex{},
	}
}

func (t *testTask) Run() error {
	if t.wg != nil {
		defer t.wg.Done()
	}

	defer func() { t.ellapsed = time.Since(t.start) }()

	if t.executeFunc != nil {
		t.start = time.Now().UTC()
		return t.executeFunc()
	}

	// if no function provided, just wait and error if told to do so
	time.Sleep(50 * time.Millisecond)
	if t.shouldErr {
		return fmt.Errorf("planned Execute() error")
	}

	return nil
}

func (t *testTask) OnFailure(e error) {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	t.failureHandled = true
}

func (t *testTask) hitFailureCase() bool {
	t.mFailure.Lock()
	defer t.mFailure.Unlock()

	return t.failureHandled
}

// ExeTime implements Task.
func (t *testTask) ExeTime() time.Duration {
	return t.ellapsed
}

func TestDeadpool_Work(t *testing.T) {
	var tasks []*testTask
	wg := &sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		tasks = append(tasks, newTestTask(
			func() error { time.Sleep(time.Nanosecond * 1); return nil },
			false,
			wg,
		),
		)
	}

	p := NewDeadpool(5, uint8(len(tasks)), loggerForTest{})

	p.Start()

	for _, j := range tasks {
		p.AddWork(j)
	}

	// we'll get a timeout failure if the tasks weren't processed
	wg.Wait()

	for taskNum, task := range tasks {
		if task.hitFailureCase() {
			t.Fatalf("error function called on task %d when it shouldn't be", taskNum)
		}
		fmt.Printf("dur: %v\n", task.ExeTime())
	}

	fmt.Printf("avg: %v\n", p.AvgExetime())
	fmt.Printf("min: %v\n", p.MinExeTime())
	fmt.Printf("max: %v\n", p.MaxExeTime())
}

type loggerForTest struct{}

func (e loggerForTest) Log(message interface{}) {
	fmt.Println(message)
}

func (e loggerForTest) Panic(message interface{}) {
	fmt.Println(message)
}

func (e loggerForTest) Fatal(message interface{}) {
	fmt.Println(message)
}

func (e loggerForTest) Error(message interface{}) {
	fmt.Println(message)
}

func (e loggerForTest) Warn(message interface{}) {
	fmt.Println(message)
}

func (e loggerForTest) Info(message interface{}) {
	fmt.Println(message)
}

func (e loggerForTest) Debug(message interface{}) {
	fmt.Println(message)
}

func (e loggerForTest) Trace(message interface{}) {
	fmt.Println(message)
}
