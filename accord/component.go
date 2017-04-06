package accord

import (
	"sync"

	"github.com/sirupsen/logrus"
)

// A Component is a background process that Accord is responsible for handling. Generally they are used for listening on a port
// and processing/responding on incoming data
type Component interface {
	// Start is called to begin the component. It is expected to return immediately (meaning it should only spawn a new goroutine)
	Start(*Accord) error

	// Stop is called to signal that a goroutine should stop processing and start to shut down. This is expected to return immediately
	// but *not* expected to have actually stopped the goroutine on return (this is so that multiple components can be triggered to stop
	// without having to wait for them sequentially)
	Stop(int)

	// WaitForStop is expected to return only after the thread has been safely stopped
	WaitForStop()
}

// ComponentRunner is a helper that is meant to be embedded in a struct to give basic Compent functionality. It starts a goroutine
// to execute in a loop and uses a "stop" and "done" channel to communicate with that goroutine.
type ComponentRunner struct {

	// stopSignal is used to signal the goroutine that it should stop
	stopSignal chan int

	// doneSignal is used by the goroutine to signify that it is closing
	doneSignal *sync.Cond

	// Helps keep us from some potential deadlocks, hopefully
	stopped bool

	stopping bool

	// Allow users of ComponentRunner to specify custom fields to be logged
	log *logrus.Entry

	accord *Accord
}

// Init takes a pointer reference to an accord struct and two functions which can make use of it.
// The 'tick' function will be called in an infinite loop in a goroutine, care should be given to this
// function to make sure it it plays fair with system resources (if left unchecked it will run unbound
// as quickly as it can) as well as returning often enough that ComponentRunner can handle shutdown
// events (otherwise it will hang). Generally, just try to make sure you sleep a bit to keep from
// flooding the CPU and that all your network requests have a timeout that gets handled.
//
// The 'cleanup' function is optional (feel free to pass in nil) and can be used to close and cleanup
// resources before the thread closes for good. There is also an optional "log" field which can be used
// to customize the logging with additional fields
//
// This function should generally be called as part of the embedding struct's Start function to get the
// process running
func (runner *ComponentRunner) Init(accord *Accord, tick func(*Accord), cleanup func(*Accord), log *logrus.Entry) {

	// We're currently only writing for cases where the runner is started and stopped once in an application
	// (start on app init, stopped on app close), but should we consider the case of somebody starting and
	// stopping multiple times? How would these variable initializations need to be changed?
	runner.stopped = false
	runner.stopSignal = make(chan int, 1)
	runner.doneSignal = sync.NewCond(&sync.Mutex{})
	runner.accord = accord

	if log != nil {
		runner.log = log
	} else {
		runner.log = accord.Logger.WithFields(logrus.Fields{})
	}

	// All the real work that ComponentRunner does happens in a goroutine, this Init function is only
	// responsible for initializing the variables and starting it
	go func() {

		// Before this goroutine returns we need to set our internal state and broadcast out to our conditional
		// variable to make anybody waiting wake up
		defer func() {
			runner.log.Info("Notifying that our goroutine is done")
			runner.doneSignal.L.Lock()
			runner.stopping = false
			runner.stopped = true
			runner.doneSignal.Broadcast()
			runner.doneSignal.L.Unlock()
		}()

		// In an infinite loop, we'll see if we have a message in our stopSignal channel and cleanup and close
		// the thread if we do (currently we don't do anything with what our stopSignal actually *is* but it's in
		// there for now just because we need to send *something* and it may be useful in the future).
		//
		// If there is no message than we call the passed in tick function. Worth stressing that this ComponentRunner
		// takes no responsibility for making sure we don't overrun the CPU or that the tick function returns
		// often enough that we can handle our Stop signals. We just have to trust that our our implementations
		// of the tick function are smart enough to handle this
		runner.log.Info("Starting component loop")
		for {
			select {
			case <-runner.stopSignal:
				runner.log.Info("Received stop signal")
				if cleanup != nil {
					runner.log.Info("Cleaning up")
					cleanup(accord)
				}
				return

			default:
				tick(accord)
			}
		}
	}()
}

// Stop implements Component's Stop method. Upon being called it will send a message to the running goroutine
// that it should start shutting down. This function returns immediately but does *not* ensure that the thread
// is actually stopped when it returns
func (runner *ComponentRunner) Stop(sig int) {
	// Hopefully this if statement will keep us from hanging forever if
	// stop is accidentally called multiple times in a row (for instance,
	// if the thread is stopped from within the thread and outside)
	if !runner.stopped || !runner.stopping {
		runner.log.Info("Sending stop signal")
		runner.stopping = true
		runner.stopSignal <- sig
		runner.log.Debug("Sent stop signal")
	}
}

// WaitForStop implements Component's WaitForStop method. It will hang until it gets a message from the running
// goroutine that it has stopped. Make sure you only use this after having already called Init and Stop, otherwise
// it will hang forever.
func (runner *ComponentRunner) WaitForStop() {
	runner.log.Info("Waiting for component to stop")
	runner.doneSignal.L.Lock()
	if !runner.stopped || runner.stopping {
		runner.doneSignal.Wait()
	}
	runner.doneSignal.L.Unlock()
	runner.log.Info("Component stopped")
}

// Shutdown offers a simple way for a component to trigger a complete shutdown of the system. This will not only shutdown
// this specific component, but the *entire* Accord system. As such, it should only be used in cases where there is an
// unrecoverable error and the only proper course of action is to panic and bring the app down
func (runner *ComponentRunner) Shutdown(err error) {
	runner.log.WithError(err).Error("Component shutting down with error")
	runner.Stop(1)
	runner.accord.Shutdown(err)
}

// ExpectedOrShutdown gives implementors a simpler way of performing error checking to see that an error is expected, otherwise
// trigger a shutdown of the system. If we determine that it is an expected error we return true
func (runner *ComponentRunner) ExpectedOrShutdown(real error, expected ...error) bool {
	match := false

	for _, err := range expected {
		if err == real {
			match = true
		}
	}
	if !match {
		runner.Shutdown(real)
	}
	return match
}
