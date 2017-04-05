package components

import (
	"bytes"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Ssawa/accord/accord"
	"github.com/beeker1121/goque"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func accordCleanup() {
	os.RemoveAll(accord.SyncFilename)
	os.RemoveAll(accord.HistoryFilename)
	os.RemoveAll(accord.StateFilename)
}

type dummyManager struct {
	local  []accord.Message
	remote []accord.Message
}

func newDummerManager() dummyManager {
	return dummyManager{
		local:  make([]accord.Message, 1),
		remote: make([]accord.Message, 1),
	}
}
func (manager dummyManager) Process(msg *accord.Message, fromRemote bool) error {
	if fromRemote {
		manager.remote = append(manager.remote, *msg)
	} else {
		manager.local = append(manager.local, *msg)
	}

	return nil
}

func (manager dummyManager) ShouldProcess(msg accord.Message, history *goque.Stack) bool {
	return true
}

func dummyAccord() *accord.Accord {
	blankLogger := &logrus.Logger{
		Out:       ioutil.Discard,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.InfoLevel,
	}

	return accord.NewAccord(newDummerManager(), nil, "", blankLogger.WithFields(nil))
}

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
	accordCleanup()
	defer accordCleanup()

	req := httptest.NewRequest("POST", "/", bytes.NewBufferString("hello, world"))
	resp := httptest.NewRecorder()

	receiver := WebReceiver{}
	accord := dummyAccord()
	accord.Start()
	receiver.Start(accord)

	receiver.mux.ServeHTTP(resp, req)
	assert.Equal(t, resp.Code, 201)

	body, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, string(body), "ok")

	receiver.Stop(0)
	receiver.WaitForStop()
	accord.Stop()
}
