package commands

import (
	"fmt"

	"github.com/piot/hasty-protocol/opath"
)

// CreateStream : CreateStream command
type CreateStream struct {
	path opath.OPath
}

// NewCreateStream : Creates a new CreateStream command
func NewCreateStream(path opath.OPath) CreateStream {
	return CreateStream{path: path}
}

// String : Returns a human readable string
func (in CreateStream) String() string {
	return fmt.Sprintf("[createstream %s]", in.path)
}

// Path : todo
func (in CreateStream) Path() opath.OPath {
	return in.path
}
