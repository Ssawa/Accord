package accord

import (
	"bytes"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWebReceiverStop(t *testing.T) {
	receiver := WebReceiver{}
	receiver.Start(dummyAccord())
	receiver.Stop(0)

	// What we're testing here is simply that our stopping logic actually works and that we don't hang.
	// As such, the main test condition we're checking is that the function ends at all. A test like this
	// is best run with something like "go test -v -timeout 900ms", to ensure that long running tests
	// fail and don't just hang forever
	receiver.WaitForStop()
}

func TestWebReceiverWaitStopTwice(t *testing.T) {
	receiver := WebReceiver{}
	receiver.Start(dummyAccord())
	receiver.Stop(0)

	receiver.WaitForStop()
	// Here we're looking to make sure that we can safely call WaitForStop twice in a row without dead locking our app
	receiver.WaitForStop()
}

func TestWebReceiverPing(t *testing.T) {
	req := httptest.NewRequest("GET", "/ping", nil)
	resp := httptest.NewRecorder()

	receiver := WebReceiver{}
	receiver.Start(dummyAccord())

	receiver.mux.ServeHTTP(resp, req)

	assert.Equal(t, resp.Code, 200)
	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, string(body), "pong")
	receiver.Stop(0)
	receiver.WaitForStop()
}

func TestWebReceiverNewCommand(t *testing.T) {
	req := httptest.NewRequest("POST", "/", bytes.NewBufferString("hello, world"))
	resp := httptest.NewRecorder()

	receiver := WebReceiver{}
	receiver.Start(dummyAccord())

	receiver.mux.ServeHTTP(resp, req)
	assert.Equal(t, resp.Code, 201)

	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, string(body), "ok")

	receiver.Stop(0)
	receiver.WaitForStop()

}
