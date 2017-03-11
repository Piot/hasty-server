package filestorage

import (
	"fmt"
	"os"
)

// ReadFile : todo
type ReadFile struct {
	file *os.File
}

// Read : reads from file
func (in ReadFile) Read(data []byte) (n int, err error) {
	return in.file.Read(data)
}

// Seek : Seeks into the file
func (in ReadFile) Seek(offset uint64) (err error) {
	clampedOffset := int64(offset)
	newSeekOffset, seekErr := in.file.Seek(clampedOffset, 0)
	if seekErr != nil {
		return seekErr
	}
	if newSeekOffset != clampedOffset {
		return fmt.Errorf("Seekposition differs %d %d", clampedOffset, newSeekOffset)
	}
	return seekErr
}

// Close : Close the file
func (in ReadFile) Close() {
	in.file.Close()
}
