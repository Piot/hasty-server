package ofilepath

import (
	"testing"

	"github.com/piot/hasty-protocol/opath"
)

func TestFileCreation(t *testing.T) {
	opath, ofpErr := opath.NewFromString("/games/@12120/users/13404")
	ofilePath, ofpErr := NewFromOPath(opath)
	if ofpErr != nil {
		t.Error(ofpErr)
	}
	if ofilePath.ToString() != "/games/12/12/0/users/13404" {
		t.Fail()
	}
}
