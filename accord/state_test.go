package accord

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateOpen(t *testing.T) {
	stateFile := "state-test"
	defer os.RemoveAll(stateFile)
	os.RemoveAll(stateFile)

	state, err := OpenState(stateFile)
	assert.Nil(t, err)
	assert.Equal(t, state.cached, uint64(0))

	_, err = os.Stat(stateFile)
	assert.Nil(t, err)
}

func TestStateSaveAndLoad(t *testing.T) {
	stateFile := "state-test"
	defer os.RemoveAll(stateFile)
	os.RemoveAll(stateFile)

	state1, err := OpenState(stateFile)
	assert.Nil(t, err)

	state1.cached = 50
	err = state1.saveToDisk()
	assert.Nil(t, err)
	state1.Close()

	state2, err := OpenState(stateFile)
	assert.Nil(t, err)

	assert.Equal(t, state2.cached, uint64(50))

}

func TestStateUpdate(t *testing.T) {
	stateFile := "state-test"
	defer os.RemoveAll(stateFile)
	os.RemoveAll(stateFile)

	state1, err := OpenState(stateFile)
	assert.Nil(t, err)

	msg1 := Message{ID: 20}
	err = state1.Update(&msg1)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), msg1.StateAt)

	msg2 := Message{ID: 30}
	err = state1.Update(&msg2)
	assert.Nil(t, err)
	assert.Equal(t, uint64(20), msg2.StateAt)

	msg3 := Message{ID: 40}
	err = state1.Update(&msg3)
	assert.Nil(t, err)
	assert.Equal(t, uint64(50), msg3.StateAt)

	state1.Close()

	state2, err := OpenState(stateFile)
	assert.Nil(t, err)

	assert.Equal(t, state2.GetCurrent(), uint64(90))
}

// func TestStateUpdateRollover(t *testing.T) {
// 	stateFile := "state-test"
// 	defer os.RemoveAll(stateFile)
// 	os.RemoveAll(stateFile)

// 	state1, err := OpenState(stateFile)
// 	assert.Nil(t, err)

// 	err = state1.Update(Message{ID: 20})
// 	assert.Nil(t, err)

// 	err = state1.Update(Message{ID: 30})
// 	assert.Nil(t, err)

// 	err = state1.Update(Message{ID: 40})
// 	assert.Nil(t, err)
// }
