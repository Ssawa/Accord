package accord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessageGenID(t *testing.T) {
	msg := Message{Timestamp: time.Time{}, Payload: []byte{0}}
	err := msg.genID()
	assert.Nil(t, err)
	assert.NotZero(t, msg.ID)
	assert.Equal(t, msg.ID, uint64(15957585615629609962))
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
