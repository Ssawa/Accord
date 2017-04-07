package components

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	"github.com/Ssawa/accord/accord"
	"github.com/stretchr/testify/assert"
)

func TestWebReceiverStop(t *testing.T) {
	receiver := WebReceiver{}
	receiver.Start(accord.DummyAccord())
	receiver.Stop(0)

	// What we're testing here is simply that our stopping logic actually works and that we don't hang.
	// As such, the main test condition we're checking is that the function ends at all. A test like this
	// is best run with something like "go test -v -timeout 900ms", to ensure that long running tests
	// fail and don't just hang forever
	receiver.WaitForStop()
}

func TestWebReceiverWaitStopTwice(t *testing.T) {
	receiver := WebReceiver{}
	receiver.Start(accord.DummyAccord())
	receiver.Stop(0)

	receiver.WaitForStop()
	// Here we're looking to make sure that we can safely call WaitForStop twice in a row without dead locking our app
	receiver.WaitForStop()
}

func TestWebReceiverPing(t *testing.T) {
	req := httptest.NewRequest("GET", "/ping", nil)
	resp := httptest.NewRecorder()

	receiver := WebReceiver{}
	defer receiver.WaitForStop()
	defer receiver.Stop(0)

	receiver.Start(accord.DummyAccord())

	receiver.mux.ServeHTTP(resp, req)

	assert.Equal(t, resp.Code, 200)
	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, string(body), "pong")
}

func TestWebReceiverNewCommand(t *testing.T) {
	accord.AccordCleanup()
	defer accord.AccordCleanup()

	req := httptest.NewRequest("POST", "/", bytes.NewBufferString("hello, world"))
	resp := httptest.NewRecorder()

	receiver := WebReceiver{}
	accord := accord.DummyAccord()
	defer receiver.WaitForStop()
	defer receiver.Stop(0)
	defer accord.Stop()

	accord.Start()
	receiver.Start(accord)

	receiver.mux.ServeHTTP(resp, req)
	assert.Equal(t, resp.Code, 201)

	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, string(body), "ok")

	status := accord.Status()
	assert.Equal(t, uint64(1), status.ToBeSyncedSize)

}

func TestWebReceiverStatus(t *testing.T) {
	accord.AccordCleanup()
	defer accord.AccordCleanup()

	req := httptest.NewRequest("GET", "/status", nil)
	resp := httptest.NewRecorder()

	receiver := WebReceiver{}
	acrd := accord.DummyAccord()

	defer receiver.WaitForStop()
	defer receiver.Stop(0)
	defer acrd.Stop()

	acrd.Start()
	receiver.Start(acrd)

	receiver.mux.ServeHTTP(resp, req)

	assert.Equal(t, resp.Code, 200)

	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)

	var status accord.Status
	err = json.Unmarshal(body, &status)
	assert.Nil(t, err)

	assert.Equal(t, uint64(0), status.HistorySize)
	assert.Equal(t, uint64(0), status.ToBeSyncedSize)
	assert.Equal(t, uint64(0), status.State)
}
