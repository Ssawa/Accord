package accord

import (
	"os"
	"os/signal"
	"path"
	"sync"

	"github.com/sirupsen/logrus"
)

const (
	// SyncFilename is where we will persist our synchronization data
	SyncFilename = "sync.queue"

	// HistoryFilename is where we will persist the history of messages we've processed for later conflict resolution
	// until we can be confident we don't need them anymore
	HistoryFilename = "history.stack"

	// StateFilename is where we will persist the internal state of our process
	StateFilename = "state.db"
)

// Status gives some insights into the current internal state of the Accord process
type Status struct {
	ToBeSyncedSize uint64
	HistorySize    uint64
	State          uint64
}

// Manager is where the majority of application specific logic should be stored and is generally
// where you can actually *use* Accord. The Accord process will call these Manager functions
// so that implementing code can make use of our synchronization system.
type Manager interface {
	// Process is called when a Message should be handled and processed. This can be called both from
	// locally created new messages or sent from a remote Accord server; this is indicated by the fromRemote
	// boolean. You should try to resolve all errors internally, returning an error will tell Accord to blow
	// up
	Process(msg Message, fromRemote bool) error

	// ShouldProcess gives the Manager a chance to filter which Messages get passed to Process so as to resolve
	// synchronization conflicts
	ShouldProcess(msg Message, history *HistoryIterator) bool
}

// Accord is the main struct responsible for maintaining state and coordinating
// all goroutines that serve for synchronizing operations
type Accord struct {

	// The logrus logger to use for outputting logs. This should be passed in
	// so that the user has fine control over how exactly data gets logged (log output, log level,
	// etc...)
	Logger *logrus.Entry

	// Path to the directory where data should be stored. This should be passed in
	// so that the user can choose where the data ges stored
	dataDir string

	// The Manager implmentation that should be used for domain specific logic. This should be passed in
	// so that the user can add application specific logic
	manager Manager

	// A list of Components that should be started and processed by Accord. This should be passed in so
	// that the implementor can choose what kind of synchronization strategies to use (or write his/her own)
	components []Component

	// ToBeSynced is used to keep track of all of the messages that need to be synchronized
	// remotely
	ToBeSynced *SyncQueue

	// history is used to keep track of the messages that were performed locally by this instance that
	// can be used for resolving merge conflicts
	history *HistoryStack

	// state is used to keep track of the internal state of our process so help detect divergence
	// with other Accord processes. It's probably a bit of overkill to use a LevelDB database to keep
	// track of our state but it's the easiest way of creating a persisted, thread safe piece of data.
	// We're already using LevelDB for goque (which is why we're not going with Bolt) and it's very
	// possible we'll want to keep track of more advanced data for our state, which this will support
	state *State

	// shutdown is a channel that can be used to communicate to the Accord process from a goroutine that
	// it should shutdown. This will generally be used by Components when they encounter an unrecoverable
	// error and the only logical course of action is to shutdown the entire application
	shutdown chan error

	// signalChannel is used to detect when a signal comes in from the operating system
	signalChannel chan os.Signal

	// We need to make sure that we don't process more than one message at a time or else our state might
	// get messed up
	processMutex *sync.Mutex
}

// NewAccord creates a new instance of Accord for you to use. This function accepts an implementation
// of the Manager interface, which will be called upon to do application specific logic. A list of
// Components, so that the user what kind of synchronization strategies to use (or write his/her own).
// The path to the directory where Accord should store its data. And a Logrus entry, so that the user
// has fine control over how exactly logs get executed (log output, log level, hooks, etc...)
func NewAccord(manager Manager, components []Component, dataDir string, logger *logrus.Entry) *Accord {
	return &Accord{
		Logger:     logger,
		dataDir:    dataDir,
		manager:    manager,
		components: components,
	}
}

// Start prepares the Accord struct and then starts up its processes. We
// return an error if there was a problem beginning the processes or creating the
// environment, otherwise we return nil.
//
// Accord does the majority of it's work using goroutines in the background, so while
// this function starts them it will return immediately; meaning this method should
// be used in tandem with something like "Listen" to keep the process alive and Accord
// processing. (These two functions are kept separate so that the user has the option
// of taking more fine grained control over their process loop. In general however,
// under normal circumstances, you'll most likely always want to follow every call to
// Start with a call to Listen which is why we offer the StartAndListen to wrap the two
// together)
func (accord *Accord) Start(signals ...os.Signal) (err error) {
	accord.Logger.Info("Initializing Accord")

	// Our first course of action should be to setup our interrupt signals, so that
	// if one comes in during our setup process we don't get stopped in the middle
	accord.signalChannel = make(chan os.Signal, 1)
	if len(signals) > 0 {
		accord.Logger.WithField("signals", signals).Info("Registering shutdown signals")
		accord.signalChannel = make(chan os.Signal, 1)
		signal.Notify(accord.signalChannel, signals...)
	}

	// Setup our internal variables and components
	accord.processMutex = &sync.Mutex{}

	accord.ToBeSynced, err = OpenSyncQueue(path.Join(accord.dataDir, SyncFilename))
	if err != nil {
		accord.Logger.WithError(err).Error("Unable to load synchronization queue")
		return err
	}

	accord.history, err = OpenHistoryStack(path.Join(accord.dataDir, HistoryFilename))
	if err != nil {
		accord.Logger.WithError(err).Error("Unable to load history stack")
		return err
	}

	accord.state, err = OpenState(path.Join(accord.dataDir, StateFilename))
	if err != nil {
		accord.Logger.WithError(err).Error("Unable to load state")
		return err
	}

	accord.shutdown = make(chan error, 1)

	accord.Logger.Info("Starting components")
	// Iterate over all of our passed in components and start them up one by one
	for _, comp := range accord.components {
		err := comp.Start(accord)
		if err != nil {
			return err
		}
	}

	return
}

// Stop safely closes down the components registered with Accord and waits for them to
// finish. This should *not* be used by components for closing Accord. Instead please use
// Shutdown
func (accord *Accord) Stop() {
	accord.Logger.Info("Stopping components")
	for _, comp := range accord.components {
		comp.Stop(0)
	}

	accord.Logger.Info("Waiting for components to stop")
	for _, comp := range accord.components {
		comp.WaitForStop()
	}

	accord.Logger.Info("Closing disk connections")
	accord.ToBeSynced.Close()
	accord.history.Close()
	accord.state.Close()
}

// Listen simply listens on our interrupt channels and hangs until one comes in. If one does,
// the Accord process is closed down cleanly
func (accord *Accord) Listen() error {
	select {
	case <-accord.signalChannel:
		accord.Logger.Info("Received OS signal")
		accord.Stop()
		return nil

	case err := <-accord.shutdown:
		accord.Logger.WithError(err).Warn("Shutting down due to error")
		accord.Stop()
		return err
	}
}

// Shutdown provides a mechanism from which components and goroutines can trigger a shutdown
// of Accord if they are in an unrecoverable state.
func (accord *Accord) Shutdown(err error) {
	accord.Logger.WithError(err).Warn("Accord is shutting down with error")
	accord.shutdown <- err
	accord.Logger.Debug("Accord sent shutdown signal")

}

// StartAndListen is a wrapper around the Init and Start functions, allowing for
// the user to completely begin the process with one function call
func (accord *Accord) StartAndListen(signals ...os.Signal) error {
	err := accord.Start(signals...)
	if err != nil {
		return err
	}

	err = accord.Listen()
	if err != nil {
		return err
	}

	return nil
}

// HandleNewMessage processes a newly created message and adds it to our queue to be
// synchronized
func (accord *Accord) HandleNewMessage(msg *Message) error {
	accord.processMutex.Lock()
	defer accord.processMutex.Unlock()

	accord.Logger.Debug("Processing a new message")
	err := accord.manager.Process(*msg, false)
	if err != nil {
		accord.Logger.WithError(err).Warn("The manager had an error while processing a message. The safest thing to do is to blow ourselves up")
		accord.Shutdown(err)
		return err
	}

	err = accord.state.Update(msg)
	if err != nil {
		accord.Logger.WithError(err).Warn("We could not update our internal state. Blowing up our application")
		accord.Shutdown(err)
		return err
	}

	err = accord.ToBeSynced.Enqueue(msg)
	if err != nil {
		accord.Logger.WithError(err).Warn("Could not save new message to our queue")
		accord.Shutdown(err)
		return err
	}

	err = accord.history.Push(msg)
	if err != nil {
		accord.Logger.WithError(err).Warn("Could not save our new message in our stack")
		accord.Shutdown(err)
		return err
	}

	return nil
}

// HandleRemoteMessage is responsible for taking a message from a remote client and updating ourselves
// to be in sync with it. It does this by checking to see if we've diverged from the remote, checking
// if our Manager thinks processing the message will cause some sort of collision with a change it has
// already made, and finally actually triggering the processing or not. In either case, we'll update our
// internal state to indicate that we handled this specific message (which will help with detecting
// divergences in the future)
func (accord *Accord) HandleRemoteMessage(msg *Message) error {
	accord.processMutex.Lock()
	defer accord.processMutex.Unlock()

	accord.Logger.Debug("Handling a remote message")

	// We first need to determine if this is something we even *should* process
	var shouldProcess bool
	if accord.state.GetCurrent() == msg.StateAt {
		// If our state matches the state the message was in when it was processed remotely than we automatically
		// know we need to process it
		accord.Logger.Debug("Our state and the remote state are synchronized, will perform the operation")
		shouldProcess = true
	} else {
		it := createHistoryIterator(accord.history)
		if accord.manager.ShouldProcess(*msg, it) {
			// If our state has diverged from the remote than we need to ask our Manager if it thinks it's safe
			// to process this message or it it will cause a collision with our update history
			accord.Logger.Debug("Our manager told us this is a process that should be processed")
			shouldProcess = true
		} else {
			// If both the previous conditions failed than we just want to ignore this particular message
			accord.Logger.Debug("Choosing not to process this message")
			shouldProcess = false
		}
		it.close()
	}

	// If we determined that we want to process this message than send it over to the Manager to do some application
	// specific operation with the data
	if shouldProcess {
		accord.Logger.Debug("Processing remote message")
		err := accord.manager.Process(*msg, true)
		if err != nil {
			accord.Logger.WithError(err).Warn("The manager had an error while processing a message. The safest thing to do is to blow ourselves up")
			accord.Shutdown(err)
			return err
		}
	}

	// Regardless of whether we actually processed the message or not we want to update our state to indicate that this specific message
	// was handled
	err := accord.state.Update(msg)
	if err != nil {
		accord.Logger.WithError(err).Warn("We could not update our internal state. Blowing up our application")
		accord.Shutdown(err)
		return err
	}

	// Our history stack really only makes sense for keeping track of those messages we actually processed, as we only use it to resolve
	// conflicts and you should never have a conflict with a message you *didn't* perform
	if shouldProcess {
		err = accord.history.Push(msg)
		if err != nil {
			accord.Logger.WithError(err).Warn("Could not save our new message in our stack")
			accord.Shutdown(err)
			return err
		}
	}

	return nil
}

// Status returns some insight into the internal metrics of the Accord process
func (accord *Accord) Status() Status {
	accord.processMutex.Lock()
	defer accord.processMutex.Unlock()

	return Status{
		ToBeSyncedSize: accord.ToBeSynced.Size(),
		HistorySize:    accord.history.Size(),
		State:          accord.state.GetCurrent(),
	}
}

// CheckRemoteState compares the passed in state with our own internal and will attempt to
// clean up our internal history using this information. If the states match we return true,
// otherwise false
func (accord *Accord) CheckRemoteState(remoteState uint64) (bool, error) {
	accord.processMutex.Lock()
	defer accord.processMutex.Unlock()

	if remoteState == accord.state.GetCurrent() {
		if accord.history.Size() > 0 {
			accord.Logger.Info("Accord processes are aligned. Clearing out history")
			err := accord.history.Clear()

			if err != nil {
				accord.Logger.WithError(err).Error("Could not clear our history")
				accord.Shutdown(err)
				return true, err
			}
		}
		return false, nil
	}

	return false, nil
}
