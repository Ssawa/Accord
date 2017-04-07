package accord

import (
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

func AccordCleanup() {
	os.RemoveAll(SyncFilename)
	os.RemoveAll(HistoryFilename)
	os.RemoveAll(StateFilename)
}

type DummyManager struct {
	local  []Message
	remote []Message
}

func NewDummerManager() DummyManager {
	return DummyManager{
		local:  make([]Message, 1),
		remote: make([]Message, 1),
	}
}
func (manager DummyManager) Process(msg Message, fromRemote bool) error {
	if fromRemote {
		manager.remote = append(manager.remote, msg)
	} else {
		manager.local = append(manager.local, msg)
	}

	return nil
}

func (manager DummyManager) ShouldProcess(msg Message, history *HistoryStack) bool {
	return true
}

func DummyAccord() *Accord {
	blankLogger := &logrus.Logger{
		Out:       ioutil.Discard,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	return NewAccord(NewDummerManager(), nil, "", blankLogger.WithFields(nil))
}
