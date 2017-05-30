package ofilepath

import (
	"testing"

	log "github.com/sirupsen/logrus"

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

func checkPath(t *testing.T, path string, valid bool) {
	opathExample, err := opath.NewFromString(path)
	if (err == nil) && !valid {
		t.Errorf("'%s' is not a valid path and should fail", path)
	} else if (err != nil) && valid {
		t.Errorf("Failed '%s' should be a valid path", path)
	}
	ofpExample, ofpErr := NewFromOPath(opathExample)
	if (ofpErr == nil) && !valid {
		t.Errorf("'%s' is not a valid path and should fail", path)
	} else if (ofpErr != nil) && valid {
		t.Errorf("Failed '%s' should be a valid path", path)
	}
	if valid {
		log.Infof("Checked '%s'", ofpExample)
	} else {
		log.Warnf("Not passed '%s'", path)
	}
}

func TestPath(t *testing.T) {
	checkPath(t, "/zaphod", true)
	checkPath(t, "/zaphod2", true)
	checkPath(t, "/32/42numbersallowedatstart", true)
	checkPath(t, "/_underscore_is_allowed", true)
	checkPath(t, "/@zaphod", true)
	checkPath(t, "/zaphod/@3", true)
	checkPath(t, "/zaphod/@01", true)

	checkPath(t, "/endslashnotallowed/", false)
	checkPath(t, "/something/åaphod", false)
	checkPath(t, "/23#@aa/23", false)
	checkPath(t, "musthaveroot", false)
	checkPath(t, "/root/endslashnotallowed/", false)
	checkPath(t, "/endslashnotallowed_/", false)
	checkPath(t, "*/", false)
	checkPath(t, "", false)
	checkPath(t, "asdf", false)
	checkPath(t, "/z aphod", false)
	checkPath(t, " /zaphod", false)
}
