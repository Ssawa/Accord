package accord

import (
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
	comp.ComponentRunner.Stop(1)
}

func TestComponentRunnerStopFromInside(t *testing.T) {
	comp := testComponentStruct{}
	comp.Start(DummyAccord())

	// This returning at all indicates that our test passes
	comp.WaitForStop()

	// Make sure that our tick was only called once
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
