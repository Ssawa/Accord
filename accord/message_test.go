package accord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessageGenID(t *testing.T) {
	msg1 := Message{Timestamp: time.Time{}, Payload: []byte{0}}
	err := msg1.genID()
	assert.Nil(t, err)
	assert.NotZero(t, msg1.ID)

	msg2 := Message{Timestamp: time.Time{}, Payload: []byte{1}}
	err = msg2.genID()
	assert.Nil(t, err)
	assert.NotZero(t, msg2.ID)

	assert.NotEqual(t, msg1, msg2)
}

func TestMessageSerializationAndDeserialization(t *testing.T) {
	msg := Message{Timestamp: time.Date(1985, time.October, 10, 26, 0, 0, 0, time.UTC), Payload: []byte{123}, StateAt: 839}
	msg.genID()

	data, err := msg.Serialize()
	assert.Nil(t, err)

	newMsg, err := DeserializeMessage(data)
	assert.Nil(t, err)

	assert.Equal(t, msg.Timestamp, newMsg.Timestamp)
	assert.Equal(t, msg.Payload, newMsg.Payload)
	assert.Equal(t, msg.ID, newMsg.ID)
	assert.Equal(t, uint64(839), msg.StateAt)
}

// I'm seeing a weird error in TestMessageGenID where calls to Serialize don't lead to the same hash, this test is mostly to help
// debug it
func TestMessageSerializationAndDeserializationMultipleTimes(t *testing.T) {
	msg1 := Message{Timestamp: time.Date(1985, time.October, 10, 26, 0, 0, 0, time.UTC), Payload: []byte{123}, StateAt: 839, ID: 80}

	data1, err := msg1.Serialize()
	assert.Nil(t, err)

	msg2, err := DeserializeMessage(data1)
	assert.Nil(t, err)

	data2, err := msg2.Serialize()
	assert.Nil(t, err)

	msg3, err := DeserializeMessage(data2)
	assert.Nil(t, err)

	data3, err := msg3.Serialize()
	assert.Nil(t, err)

	msg4, err := DeserializeMessage(data3)
	assert.Nil(t, err)

	data4, err := msg4.Serialize()
	assert.Nil(t, err)

	assert.Equal(t, msg1.Timestamp, msg2.Timestamp)
	assert.Equal(t, msg1.Payload, msg2.Payload)
	assert.Equal(t, msg1.ID, msg2.ID)
	assert.Equal(t, msg1.StateAt, msg2.StateAt)

	assert.Equal(t, msg1.Timestamp, msg3.Timestamp)
	assert.Equal(t, msg1.Payload, msg3.Payload)
	assert.Equal(t, msg1.ID, msg3.ID)
	assert.Equal(t, msg1.StateAt, msg3.StateAt)

	assert.Equal(t, msg1.Timestamp, msg4.Timestamp)
	assert.Equal(t, msg1.Payload, msg4.Payload)
	assert.Equal(t, msg1.ID, msg4.ID)
	assert.Equal(t, msg1.StateAt, msg4.StateAt)

	assert.Equal(t, data1, data2)
	assert.Equal(t, data1, data3)
	assert.Equal(t, data1, data4)
}

// Like TestMessageSerializationAndDeserializationMultipleTimes, this test is meant to help test whether we are assured
// the same message id given the same data
func TestMessageGenIDMultipleTimes(t *testing.T) {
	msg1 := Message{Timestamp: time.Time{}, Payload: []byte{0}}
	msg2 := Message{Timestamp: time.Time{}, Payload: []byte{0}}

	err := msg1.genID()
	assert.Nil(t, err)

	msg2.Serialize()
	err = msg2.genID()
	assert.Nil(t, err)

	assert.Equal(t, msg1.ID, msg2.ID)

	_, err = msg1.Serialize()
	assert.Nil(t, err)

	err = msg1.genID()
	assert.Nil(t, err)

	assert.Equal(t, msg1.ID, msg2.ID)

}

func TestMessageNewerThan(t *testing.T) {
	msg1 := Message{Timestamp: time.Date(1985, time.October, 10, 26, 0, 0, 0, time.UTC), Payload: []byte{123}, StateAt: 839}
	msg2 := Message{Timestamp: time.Date(1955, time.October, 10, 26, 0, 0, 0, time.UTC), Payload: []byte{123}, StateAt: 839}

	assert.True(t, msg1.NewerThan(msg2))
	assert.False(t, msg2.NewerThan(msg1))
}

func TestMessageOlderThan(t *testing.T) {
	msg1 := Message{Timestamp: time.Date(1985, time.October, 10, 26, 0, 0, 0, time.UTC), Payload: []byte{123}, StateAt: 839}
	msg2 := Message{Timestamp: time.Date(1955, time.October, 10, 26, 0, 0, 0, time.UTC), Payload: []byte{123}, StateAt: 839}

	assert.False(t, msg1.OlderThan(msg2))
	assert.True(t, msg2.OlderThan(msg1))
}
