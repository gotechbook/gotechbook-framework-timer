package timer

import (
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"sync"
	"sync/atomic"
	"time"
)

var timerBacklog int

const (
	LoopForever = -1
)

var (
	Manager = &struct {
		incrementID    int64      // auto increment id
		timers         sync.Map   // all Timers
		ChClosingTimer chan int64 // timer for closing
		ChCreatedTimer chan *Timer
	}{}

	// Precision indicates the precision of timer, default is time.Second
	Precision = time.Second

	// GlobalTicker represents global ticker that all cron job will be executed
	// in globalTicker.
	GlobalTicker *time.Ticker
)

type (
	Func func()

	// Condition represents a checker that returns true when cron job needs
	Condition interface {
		Check(now time.Time) bool
	}

	// Timer represents a cron job
	Timer struct {
		ID        int64         // timer id
		fn        Func          // function that execute
		createAt  int64         // timer create time
		interval  time.Duration // execution interval
		condition Condition     // condition to cron job execution
		elapse    int64         // total elapse time
		closed    int32         // is timer closed
		counter   int           // counter
	}
)

func init() {
	// since this runs on init it is better to leave the value hardcoded here
	timerBacklog = 1 << 8
	Manager.ChClosingTimer = make(chan int64, timerBacklog)
	Manager.ChCreatedTimer = make(chan *Timer, timerBacklog)
}

// AddTimer adds a timer to the manager
func AddTimer(t *Timer) {
	Manager.timers.Store(t.ID, t)
}

// RemoveTimer removes a timer to the manager
func RemoveTimer(id int64) {
	Manager.timers.Delete(id)
}

// NewTimer creates a cron job
func NewTimer(fn Func, interval time.Duration, counter int) *Timer {
	id := atomic.AddInt64(&Manager.incrementID, 1)
	t := &Timer{
		ID:       id,
		fn:       fn,
		createAt: time.Now().UnixNano(),
		interval: interval,
		elapse:   int64(interval), // first execution will be after interval
		counter:  counter,
	}

	// add to manager
	Manager.ChCreatedTimer <- t
	return t
}

// SetCondition sets the condition used for verifying when the cron job should run
func (t *Timer) SetCondition(condition Condition) {
	t.condition = condition
}

// Stop turns off a timer. After Stop, fn will not be called forever
func (t *Timer) Stop() {
	if atomic.LoadInt32(&t.closed) > 0 {
		return
	}

	// guarantee that logic is not blocked
	if len(Manager.ChClosingTimer) < timerBacklog {
		Manager.ChClosingTimer <- t.ID
		atomic.StoreInt32(&t.closed, 1)
	} else {
		t.counter = 0 // automatically closed in next Cron
	}
}

// execute job function with protection
func pexec(id int64, fn Func) {
	defer func() {
		if err := recover(); err != nil {
			logger.Log.Errorf("Call timer function error, TimerID=%d, Error=%v", id, err)
		}
	}()

	fn()
}

func Cron() {
	now := time.Now()
	unn := now.UnixNano()
	Manager.timers.Range(func(idInterface, tInterface interface{}) bool {
		t := tInterface.(*Timer)
		id := idInterface.(int64)
		// prevent ChClosingTimer exceed
		if t.counter == 0 {
			if len(Manager.ChClosingTimer) < timerBacklog {
				t.Stop()
			}
			return true
		}

		// condition timer
		if t.condition != nil {
			if t.condition.Check(now) {
				pexec(id, t.fn)
			}
			return true
		}

		// execute job
		if t.createAt+t.elapse <= unn {
			pexec(id, t.fn)
			t.elapse += int64(t.interval)

			// update timer counter
			if t.counter != LoopForever && t.counter > 0 {
				t.counter--
			}
		}
		return true
	})
}

// SetTimerBacklog set the timer created/closing channel backlog, A small backlog
// may cause the logic to be blocked when call NewTimer/NewCountTimer/timer.Stop
// in main logic gorontine.
func SetTimerBacklog(c int) {
	if c < 16 {
		c = 16
	}
	timerBacklog = c
}
