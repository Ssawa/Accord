package accord

import (
	"errors"
	"syscall"
	"testing"
	"time"

	zmq "github.com/pebbe/zmq4"

	"github.com/stretchr/testify/assert"
)

func TestComponentRunner(t *testing.T) {
	ticked := false
	cleanedUp := false
	tick := func(*Accord) { ticked = true }
	cleanup := func(*Accord) { cleanedUp = true }

	runner := ComponentRunner{}
	runner.Init(DummyAccord(), tick, cleanup, nil)
	time.Sleep(time.Millisecond)
	runner.Stop(0)
	runner.WaitForStop()
	assert.True(t, ticked)
	assert.True(t, cleanedUp)
}

func TestComponentRunnerWaitStopTwice(t *testing.T) {
	runner := ComponentRunner{}
	runner.Init(DummyAccord(), func(*Accord) {}, nil, nil)
	runner.Stop(0)
	runner.WaitForStop()
	runner.WaitForStop()
}

type testComponentStruct struct {
	ComponentRunner
	runCount int
}

func (comp *testComponentStruct) Start(accord *Accord) {
	comp.runCount = 0
	comp.ComponentRunner.Init(accord, comp.tick, nil, nil)
}

func (comp *testComponentStruct) tick(*Accord) {
	comp.runCount++
	comp.Shutdown(nil)
}

func TestComponentRunnerShutdown(t *testing.T) {
	AccordCleanup()
	defer AccordCleanup()

	comp := testComponentStruct{}
	accord := DummyAccord()
	accord.Start()

	comp.Start(accord)

	// This returning at all indicates that our test passes
	comp.WaitForStop()

	// Make sure that our tick was only called once
	assert.Equal(t, comp.runCount, 1)
}

type testComponentStructMultiple struct {
	testComponentStruct
}

func (comp *testComponentStructMultiple) tick(*Accord) {
	comp.runCount++
	comp.Shutdown(nil)
	comp.Shutdown(nil)
}

// Make sure we can't accidentally get into dead locks
func TestComponentRunnerShutdownMultiple(t *testing.T) {
	AccordCleanup()
	defer AccordCleanup()

	comp := testComponentStructMultiple{}
	accord := DummyAccord()
	accord.Start()

	comp.Start(accord)

	comp.WaitForStop()

	assert.Equal(t, comp.runCount, 1)
}

type testComponentZMQ struct {
	ComponentRunner
	t           *testing.T
	sockSend    *zmq.Socket
	sockReceive *zmq.Socket

	runOnce bool
}

func (comp *testComponentZMQ) Start(accord *Accord) {
	comp.runOnce = false

	comp.sockSend, _ = zmq.NewSocket(zmq.REQ)
	comp.sockReceive, _ = zmq.NewSocket(zmq.REP)

	comp.sockSend.Bind("inproc://send")
	comp.sockReceive.Bind("inproc://receive")

	comp.sockSend.SetSndtimeo(time.Millisecond)
	comp.sockReceive.SetRcvtimeo(time.Millisecond)

	comp.ComponentRunner.Init(accord, comp.tick, comp.cleanup, nil)
}

func (comp *testComponentZMQ) tick(*Accord) {
	comp.runOnce = true

	i, err := comp.sockSend.Send("hello", 0)

	assert.Equal(comp.t, i, -1)
	assert.NotNil(comp.t, err)
	assert.Equal(comp.t, err, zmq.Errno(syscall.EAGAIN))

	s, err := comp.sockReceive.Recv(0)
	assert.Empty(comp.t, s)
	assert.NotNil(comp.t, err)
}

func (comp *testComponentZMQ) cleanup(*Accord) {
	comp.sockSend.Close()
	comp.sockReceive.Close()
}

// For my own sanity, let's just make sure that ZMQ actually works
// with this system (as we should be using it a lot)
func TestZMQRunnerStop(t *testing.T) {
	comp := testComponentZMQ{t: t}
	comp.Start(DummyAccord())
	time.Sleep(time.Millisecond)
	comp.Stop(0)
	comp.WaitForStop()

	assert.True(t, comp.runOnce)
}

var errKnown1 = errors.New("KNOWN ERROR1")
var errKnown2 = errors.New("KNOWN ERROR2")

type shutdownRunner struct {
	ComponentRunner
	errs     []error
	runCount int
}

func (run *shutdownRunner) Start(acrd *Accord) error {
	run.runCount = 0
	run.Init(acrd, run.tick, nil, nil)
	return nil
}

func (run *shutdownRunner) tick(*Accord) {
	run.runCount++

	var err error
	err, run.errs = run.errs[0], run.errs[1:]
	run.ExpectedOrShutdown(err, errKnown1, errKnown2)
	return
}

func TestComponentRunnerExpectedOrShutdown(t *testing.T) {
	AccordCleanup()
	defer AccordCleanup()

	acrd := DummyAccord()
	acrd.Start()
	runner := shutdownRunner{errs: []error{errKnown2, errKnown1, errors.New("STRANGE ERROR")}}
	runner.Start(acrd)

	runner.WaitForStop()

	assert.Equal(t, 3, runner.runCount)
}
