package engines

import "gitlab.com/yaotsu/core"

// A SerialEngine is an Engine that always run events one after another.
type SerialEngine struct {
	*core.HookableBase

	time   core.VTimeInSec
	paused bool
	queue  EventQueue
}

// NewSerialEngine creates a SerialEngine
func NewSerialEngine() *SerialEngine {
	e := new(SerialEngine)
	e.HookableBase = core.NewHookableBase()

	e.paused = false

	e.queue = NewEventQueue()

	return e
}

// Schedule register an event to be happen in the future
func (e *SerialEngine) Schedule(evt core.Event) {
	e.queue.Push(evt)
}

// Run processes all the events scheduled in the SerialEngine
func (e *SerialEngine) Run() error {
	for !e.paused {
		if e.queue.Len() == 0 {
			return nil
		}

		evt := e.queue.Pop()

		e.time = evt.Time()

		e.InvokeHook(evt, e, core.BeforeEvent, nil)
		handler := evt.Handler()
		handler.Handle(evt)
		e.InvokeHook(evt, e, core.AfterEvent, nil)
	}

	return nil
}

// Pause will stop the engine from dispatching more events
func (e *SerialEngine) Pause() {
	e.paused = true
}

// CurrentTime returns the current time at which the engine is at. Specifically, the run time of the current event.
func (e *SerialEngine) CurrentTime() core.VTimeInSec {
	return e.time
}
