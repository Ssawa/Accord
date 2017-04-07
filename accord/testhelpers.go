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
	Local              []Message
	Remote             []Message
	ProcessCount       int
	ShouldProcessCount int

	ShouldProcessRet bool
}

func NewDummerManager() *DummyManager {
	return &DummyManager{
		Local:  make([]Message, 1),
		Remote: make([]Message, 1),
	}
}
func (manager *DummyManager) Process(msg Message, fromRemote bool) error {
	manager.ProcessCount++

	if fromRemote {
		manager.Remote = append(manager.Remote, msg)
	} else {
		manager.Local = append(manager.Local, msg)
	}

	return nil
}

func (manager *DummyManager) ShouldProcess(msg Message, history *HistoryStack) bool {
	manager.ShouldProcessCount++
	return manager.ShouldProcessRet
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

func DummyAccordManager(manager Manager) *Accord {
	accord := DummyAccord()
	accord.manager = manager
	return accord
}
