package accord

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"time"
)

// Message represents a an arbitrary message that should be propagated and synchronized throughout the system
type Message struct {
	// An identifier for this message that should be unique based both on the content of the message as well
	// as the time it was created
	ID uint64

	// The UTC timestamp that the message was created. Obviously in a distributed environment timestamps are more
	// of a suggestion rather than a hard truth but it's the best we've got
	Timestamp time.Time

	// StateAt represents the state of the Message's originating Accord process when it was processed
	StateAt uint64

	// The actual content of the message. Our system should make as little assumptions about this as possible
	// and instead leave application specific logic to implementors
	Payload []byte
}

// NewMessage crafts a new Message using the passed in payload. This should only be for creating *bew* Message
// (*not* deserializing Messages that get passed over the network, for that look at DeserializeMessage)
func NewMessage(payload []byte) (*Message, error) {

	// Create our initial bundle of data
	msg := &Message{
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	// Use our bundle of data to generate our ID, which is dependant on the previous fields
	err := msg.genID()
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// DeserializeMessage takes a byte slice and parses it back into a Message struct. This should be used along
// with the Serialize method to send Messages over the wire
func DeserializeMessage(data []byte) (*Message, error) {
	decoder := gob.NewDecoder(bytes.NewReader(data))
	msg := Message{}
	err := decoder.Decode(&msg)

	if err != nil {
		return nil, err
	}

	return &msg, nil
}

// genID takes a partially constructed Message and generates an identification using the present
// fields
func (msg *Message) genID() error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	// Are there any other fields we should make our ID dependant on? We can't use StateAt because
	// it doesn't get set until our message *actually* gets executed
	err := encoder.Encode(struct {
		Timestamp time.Time
		Payload   []byte
	}{msg.Timestamp, msg.Payload})

	if err != nil {
		return err
	}

	hasher := sha256.New()
	hasher.Write(buf.Bytes())
	hash := hasher.Sum(nil)

	// We could *technically* just use the hash as our ID but we don't really need 256 bits of entropy
	// and it would just make some of our arithmetic down the road more complicated and slower, so for
	// now let's save oursize a few bytes every message and make our lives a bit easier later
	msg.ID = binary.LittleEndian.Uint64(hash)

	return nil
}

// Serialize encodes the Message into a byte slice so that it can be transported over a network. The DeserializeMessage
// function can subsequently be used to recreate the Message
func (msg *Message) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(*msg)

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
