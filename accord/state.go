package accord

import (
	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

const (
	stateKey = "state"
)

// State represents the internal history of Accord. Our state is essentially just a cumulative function of
// every Message we have have processed from which we can use to determine if we've diverged from our remote
// client
type State struct {
	// db is where we store and persist all of our state data. It's probably a bit of overkill to use a
	// LevelDB database to keep track of our state but it's the easiest way of creating a persisted, thread
	// safe piece of data. We're already using LevelDB for goque (which is why we're not going with Bolt)
	// and it's very possible we'll want to keep track of more advanced data for our state, which this will
	// help us support
	db *leveldb.DB

	// Theres no point for us to go to the disk everytime we want to know our state as long as we can ensure
	// we're the only ones updating it
	cached uint64
}

// OpenState will open or create a LevelDB database that stores our state information and then load and cache
// our data for reads. Will return an error if any occur during this process
func OpenState(path string) (*State, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	state := State{db: db}

	err = state.loadFromDisk()
	if err != nil {
		return nil, err
	}

	return &state, nil
}

// Close and clean up our LevelDB connection
func (state *State) Close() {
	state.db.Close()
}

// loadFromDisk gets our data out of LevelDB and caches it in memory
func (state *State) loadFromDisk() error {
	val, err := state.db.Get([]byte(stateKey), nil)

	// This is a bit busy but essentially we're just checking to see if we got
	// an error from our database read. If we did, but it's because the key could
	// not be found, then use a default value, otherwise return the error. If we
	// didn't get any error at all, then just use the value returned
	if err != nil {
		if err == errors.ErrNotFound {
			state.cached = 0
		} else {
			return err
		}
	} else {
		state.cached = binary.LittleEndian.Uint64(val)
	}

	return nil
}

// saveToDisk saves our instance to disk as it currently is so that it can
// be persisted
func (state *State) saveToDisk() error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, state.cached)

	return state.db.Put([]byte(stateKey), data, nil)
}

// GetCurrent returns our current state
func (state *State) GetCurrent() uint64 {
	return state.cached
}

// Update updates our current state to signify that a message has been
// processed by our system
func (state *State) Update(msg Message) error {
	original := state.cached

	state.cached += msg.ID

	err := state.saveToDisk()
	if err != nil {
		state.cached = original
		return err
	}

	return nil
}
