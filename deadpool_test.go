package deadpool

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func Test_deadpool_ErrsOnCreate(t *testing.T) {
	_, err := New(WithMax(-1))

	if err == nil {
		t.Log("ErrInvalidMaxWorkers expected")
		t.FailNow()
	}

	_, err = New(WithCap(-1))

	if err == nil {
		t.Log("ErrInvalidCap expected")
		t.FailNow()
	}

	_, err = New(WithExecutor(nil))

	if err == nil {
		t.Log("ErrInvalidExecutor expected")
		t.FailNow()
	}

	_, err = New(WithSpawner(nil))

	if err == nil {
		t.Log("ErrInvalidSpawner expected")
		t.FailNow()
	}
}

func Test_deadpool_Flow(t *testing.T) {
	//	os.Setenv("DEBUG", "TRUE")

	d, err := New(WithMax(6))

	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	start := time.Now().UTC()

	for i := 1; i <= 128; i++ {
		func(i int) {
			d.Submit(newTaskTest(i))
		}(i)
	}

	d.WaitAll()

	t.Logf("tiempo acumulado de proceso: %v\n", d.CummlatedWorkTime())
	t.Logf("tiempo promedio por tarea: %v \n", d.AVGTime())
	t.Logf("tiempo total de proceso: %v", time.Since(start))
}

type taskTest struct {
	id int
}

func newTaskTest(id int) *taskTest {
	return &taskTest{
		id: id,
	}
}

func (t *taskTest) Run() {
	defer func() {
		if os.Getenv("DEBUG") == "TRUE" {
			fmt.Printf("     stoping task %d\n", t.id)
		}
	}()

	if os.Getenv("DEBUG") == "TRUE" {
		fmt.Printf("     starting task %d\n", t.id)
		time.Sleep(10 * time.Millisecond)
	}
}
