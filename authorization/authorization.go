package authorization

// Authorization : todo
type Authorization struct {
	canWrite bool
	canRead  bool
}

// AllowedToWrite : todo
func (in Authorization) AllowedToWrite() bool {
	return in.canWrite
}

// NewAuthorization : todo
func NewAuthorization(canRead bool, canWrite bool) Authorization {
	return Authorization{canRead: canRead, canWrite: canWrite}
}
