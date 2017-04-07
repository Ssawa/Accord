package accord

import (
	"errors"
	"os"
	"os/signal"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type testingWriter struct {
	previous *[]string
}

func createTestingWriter() testingWriter {
	return testingWriter{new([]string)}
}

func (writer testingWriter) Write(p []byte) (n int, err error) {
	*writer.previous = append(*writer.previous, string(p))
	return len(p), nil
}

func TestAccordLogging(t *testing.T) {
	defer AccordCleanup()

	writer := createTestingWriter()

	logger := logrus.New()
	logger.Out = writer

	accord := Accord{
		Logger: logger.WithFields(nil),
	}

	accord.Start()
	assert.True(t, len(*writer.previous) >= 1)
}

type noopComponent struct {
	started bool
	stopped bool
}

func (noop *noopComponent) Start(accord *Accord) (err error) {
	noop.started = true
	return
}

func (noop *noopComponent) Stop(int) {
	noop.stopped = true
}

func (noop *noopComponent) WaitForStop() {
}

func TestAccordComponentStart(t *testing.T) {
	defer AccordCleanup()

	comp1 := &noopComponent{}
	comp2 := &noopComponent{}
	comp3 := &noopComponent{}

	accord := DummyAccord()
	accord.components = []Component{comp1, comp2, comp3}
	err := accord.Start()
	assert.Nil(t, err)

	assert.True(t, comp1.started)
	assert.True(t, comp2.started)
	assert.True(t, comp3.started)
}

type noopComponentError struct {
	noopComponent
}

func (noop *noopComponentError) Start(accord *Accord) (err error) {
	return errors.New("Manufactured Error")
}

func TestAccordComponentStartError(t *testing.T) {
	defer AccordCleanup()

	comp1 := &noopComponent{}
	comp2 := &noopComponent{}
	comp3 := &noopComponentError{}

	accord := DummyAccord()
	accord.components = []Component{comp1, comp2, comp3}
	err := accord.Start()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "Manufactured Error")
}

func TestAccordComponentStop(t *testing.T) {
	defer AccordCleanup()

	comp1 := &noopComponent{}
	comp2 := &noopComponent{}
	comp3 := &noopComponent{}

	accord := DummyAccord()
	accord.components = []Component{comp1, comp2, comp3}
	err := accord.Start()
	assert.Nil(t, err)

	accord.Stop()

	assert.True(t, comp1.stopped)
	assert.True(t, comp2.stopped)
	assert.True(t, comp3.stopped)

}

func TestAccordStartOSShutdown(t *testing.T) {
	defer AccordCleanup()
	defer signal.Reset()

	comp1 := &noopComponent{}
	comp2 := &noopComponent{}
	comp3 := &noopComponent{}

	accord := DummyAccord()
	accord.components = []Component{comp1, comp2, comp3}

	accord.Start(os.Interrupt)

	done := make(chan error, 1)

	go func() {
		done <- accord.Listen()
	}()

	pid := os.Getpid()
	proc, err := os.FindProcess(pid)
	assert.Nil(t, err)

	err = proc.Signal(os.Interrupt)
	assert.Nil(t, err)

	<-done

	assert.True(t, comp1.started)
	assert.True(t, comp2.started)
	assert.True(t, comp3.started)

	assert.True(t, comp1.stopped)
	assert.True(t, comp2.stopped)
	assert.True(t, comp3.stopped)

}

func TestAccordStartErrorShutdown(t *testing.T) {
	defer AccordCleanup()

	comp1 := &noopComponent{}
	comp2 := &noopComponent{}
	comp3 := &noopComponent{}

	accord := DummyAccord()
	accord.components = []Component{comp1, comp2, comp3}

	accord.Start()

	done := make(chan error, 1)

	go func() {
		done <- accord.Listen()
	}()

	accord.Shutdown(errors.New("test error"))
	err := <-done
	assert.Equal(t, err.Error(), "test error")

	assert.True(t, comp1.started)
	assert.True(t, comp2.started)
	assert.True(t, comp3.started)

	assert.True(t, comp1.stopped)
	assert.True(t, comp2.stopped)
	assert.True(t, comp3.stopped)

}

func TestAccordMultipleNewOperations(t *testing.T) {
	defer AccordCleanup()
	accord := DummyAccord()
	accord.Start()

	msg1 := &Message{ID: 1}
	msg2 := &Message{ID: 2}
	msg3 := &Message{ID: 3}
	msg4 := &Message{ID: 4}
	msg5 := &Message{ID: 5}

	done := make(chan int, 5)

	execute := func(msg *Message) {
		accord.HandleNewMessage(msg)
		done <- 0
	}

	go execute(msg1)
	go execute(msg2)
	go execute(msg3)
	go execute(msg4)
	go execute(msg5)

	for i := 0; i < 5; i++ {
		<-done
	}

	assert.Equal(t, uint64(15), accord.state.cached)
	assert.Equal(t, uint64(5), accord.ToBeSynced.Size())
	assert.Equal(t, uint64(5), accord.history.Size())
}

func TestAccordHandleRemoteOperation(t *testing.T) {
	defer AccordCleanup()
	accord := DummyAccord()

	manager := DummyManager{ShouldProcessRet: true}
	accord.manager = &manager

	accord.Start()
	defer accord.Stop()

	// Set our states to the same thing
	accord.state.cached = 1
	msg := &Message{ID: 4, StateAt: 1}

	err := accord.HandleRemoteMessage(msg)
	assert.Nil(t, err)

	// Our states are the same so we shouldn't be asked if we should process but we should also actually process
	assert.Equal(t, 0, manager.ShouldProcessCount)
	assert.Equal(t, 1, manager.ProcessCount)
	assert.Equal(t, uint64(1), accord.history.Size())
	assert.Equal(t, uint64(5), accord.state.GetCurrent())

	// Diverging states
	msg = &Message{ID: 10, StateAt: 100}
	err = accord.HandleRemoteMessage(msg)
	assert.Nil(t, err)

	// Our states are the same and we're returning that we should process
	assert.Equal(t, 1, manager.ShouldProcessCount)
	assert.Equal(t, 2, manager.ProcessCount)
	assert.Equal(t, uint64(2), accord.history.Size())
	assert.Equal(t, uint64(15), accord.state.GetCurrent())

	// Diverging states, not processing
	manager.ShouldProcessRet = false
	msg = &Message{ID: 5, StateAt: 30}
	err = accord.HandleRemoteMessage(msg)
	assert.Nil(t, err)

	// Our states are the same and we're saying we shouldn't process
	assert.Equal(t, 2, manager.ShouldProcessCount)
	assert.Equal(t, 2, manager.ProcessCount)
	assert.Equal(t, uint64(2), accord.history.Size())
	assert.Equal(t, uint64(20), accord.state.GetCurrent())
}

func TestAccordCheckRemoteState(t *testing.T) {
	defer AccordCleanup()
	accord := DummyAccord()

	accord.Start()
	defer accord.Stop()

	accord.history.Push(&Message{ID: 1})
	assert.Equal(t, uint64(1), accord.history.Size())

	val, err := accord.CheckRemoteState(accord.state.GetCurrent() + 1)
	assert.Nil(t, err)
	assert.False(t, val)
	assert.Equal(t, uint64(1), accord.history.Size())

	val, err = accord.CheckRemoteState(accord.state.GetCurrent())
	assert.Nil(t, err)
	assert.False(t, val)
	assert.Equal(t, uint64(0), accord.history.Size())
}
