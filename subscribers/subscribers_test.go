package subscribers

import (
	"log"
	"testing"

	"github.com/piot/hasty-protocol/opath"
)

type NullSubscriber struct {
	lastPath opath.OPath
}

func (in *NullSubscriber) EntityChanged(path opath.OPath) {
	log.Printf("NullSubscriber knows that %s changed", path)
	in.lastPath = path
}

func TestSomeSubscriptions(t *testing.T) {
	s := NewSubscribers()
	path, _ := opath.NewFromString("/test/world")
	testSubscriber := NullSubscriber{}
	s.AddSubscriber(path, &testSubscriber)
	s.EntityChanged(path)
	if testSubscriber.lastPath != path {
		t.Errorf("path differs:%s", testSubscriber.lastPath)
	}
	anotherPath, _ := opath.NewFromString("/test/eliza")
	firstRemoveErr := s.RemoveSubscriber(path, &testSubscriber)
	if firstRemoveErr != nil {
		t.Errorf("Remove err:%s", firstRemoveErr)
	}
	s.EntityChanged(anotherPath)
	if testSubscriber.lastPath != path {
		t.Errorf("path differs:%s", testSubscriber.lastPath)
	}
	secondRemoveErr := s.RemoveSubscriber(path, &testSubscriber)
	if secondRemoveErr == nil {
		t.Errorf("This should result in an error")
	}
}
