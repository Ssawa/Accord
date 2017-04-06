package components

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/Ssawa/accord/accord"
	"github.com/sirupsen/logrus"
)

// WebReceiver is a Component that is responsible for starting an HTTP server and ingesting
// incoming local commands. While the main functionality is to allow for the insertion of
// new commands into the system, it should be thought more broadly as the general point
// of interaction for non Accord programs (hence why it uses HTTP rather than ZMQ).
// Additional functionality we may wish to include are "ping" endpoints to allow external
// clients to test that the Accord application is up or "status" endpoints, to allow
// external clients to introspect into the state of the system (size of queue, synchronization
// status, etc). We may also choose to allow simple management actions to be taken, such as
// clearing the queue or resetting our internal state.
//
// It's important to note that we take no pains to safeguard this http endpoint with even the
// most basic of authentication. Meaning that the implementor should exercise caution to make
// sure that the server is only bound to localhost or, if exposed to the internet, behind
// a reverse proxy (such as nginx) so that basic authentication and TLS can be added.
type WebReceiver struct {

	// The address the HTTP server should bind to
	BindAddress string

	server     *http.Server
	mux        *http.ServeMux
	stopSignal *sync.Cond
	stopping   bool
	accord     *accord.Accord
	log        *logrus.Entry
}

// Start initializes our web routes and starts the HTTP server (it does *not*, however, assure
// that the port is completely bound and listening at the time it returns, as this occurs in a
// background thread)
func (receiver *WebReceiver) Start(accord *accord.Accord) (err error) {
	// Save a reference to our accord instance so we can use it within our handlers
	receiver.accord = accord
	receiver.log = accord.Logger.WithField("component", "WebReceiver")

	// Will be used much the same way as ComponentRunner, to signal when the background thread
	// has been cleanly shutdown
	receiver.stopSignal = sync.NewCond(&sync.Mutex{})

	receiver.mux = http.NewServeMux()

	// Register our routes
	receiver.mux.HandleFunc("/", receiver.newCommand)
	receiver.mux.HandleFunc("/ping", receiver.ping)
	receiver.mux.HandleFunc("/status", receiver.status)

	// Start our server in a background thread so that we don't block
	receiver.server = &http.Server{Addr: receiver.BindAddress, Handler: receiver.mux}

	receiver.log.WithField("address", receiver.BindAddress).Info("Starting HTTP server")
	go receiver.server.ListenAndServe()

	return
}

// Stop begins the process of shutting down our running HTTP server and returns
func (receiver *WebReceiver) Stop(int) {
	go func() {
		receiver.log.Info("Shutting down HTTP server")
		receiver.stopping = true
		receiver.server.Shutdown(nil)
		receiver.stopping = false
		receiver.stopSignal.Broadcast()
		receiver.log.Info("HTTP server safely shutdown")
	}()
	return
}

// WaitForStop waits and listens for the process begun by our "Stop" function finishes, indicating
// that the HTTP server has cleanly shutdown
func (receiver *WebReceiver) WaitForStop() {
	if receiver.stopping {
		receiver.stopSignal.L.Lock()
		receiver.stopSignal.Wait()
		receiver.stopSignal.L.Unlock()
	}
}

// newCommand performs the main role of WebReceiver, it takes data sent in through
// a web request, wraps it in a Message struct, and sends it off to Accord to handle.
// Upon success it returns a 201 with an "ok" message.
//
// Note that this message does *not* transport Message structs, it *creates* new ones
// using the passed in data as a payload
func (receiver *WebReceiver) newCommand(w http.ResponseWriter, r *http.Request) {
	receiver.log.Debug("Received a new command request")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		receiver.log.WithError(err).Warn("Error parsing new message")
		http.Error(w, err.Error(), 500)
		return
	}

	msg, err := accord.NewMessage(body)
	if err != nil {
		receiver.log.WithError(err).Warn("Error generating a new message")
		http.Error(w, err.Error(), 500)
		return
	}

	err = receiver.accord.HandleNewMessage(msg)
	if err != nil {
		receiver.log.WithError(err).Warn("Error handling new message")
		http.Error(w, err.Error(), 500)
		return
	}

	receiver.log.Debug("New command successfully handled")
	w.WriteHeader(201)
	w.Write([]byte("ok"))
}

// pingHandler is responsible for sending back a small response upon any kind of request to indicate
// that we're still alive
func (receiver *WebReceiver) ping(w http.ResponseWriter, r *http.Request) {
	receiver.log.Debug("Ping request")
	w.Write([]byte("pong"))
}

// status is a handler that let's external applications see the internal status of Accord
func (receiver *WebReceiver) status(w http.ResponseWriter, r *http.Request) {
	status := receiver.accord.Status()
	data, err := json.Marshal(status)
	if err != nil {
		receiver.log.WithError(err).Warn("Error encoding status to json")
		http.Error(w, err.Error(), 500)
		return
	}

	w.Write(data)
}
