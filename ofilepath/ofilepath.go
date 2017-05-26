package ofilepath

import (
	"fmt"
	"strings"

	"github.com/piot/hasty-protocol/opath"
)

// OFilePath : Path to an object or stream
type OFilePath struct {
	path opath.OPath
}

func splitDirectory(dir string) string {
	dirLength := len(dir)
	result := ""
	for i := 0; i < dirLength-1; i += 2 {
		charsToPick := 2
		snip := dir[i : i+charsToPick]
		if i != 0 {
			result += "/"
		}
		result += snip
	}
	if dirLength%2 == 1 {
		if len(result) > 0 {
			result += "/"
		}
		result += dir[dirLength-1 : dirLength]
	}
	return result
}

func splitIntoFileSystemFriendlyPaths(op opath.OPath) (opath.OPath, error) {
	str := op.ToString()
	parts := strings.Split(str, "/")

	var newParts []string
	for index, directory := range parts {
		if index == 0 {
			if len(directory) != 0 {
				return opath.OPath{}, fmt.Errorf("Must start with /")
			}
		} else if len(directory) == 0 {
			return opath.OPath{}, fmt.Errorf("Not allowed with zero length paths")
		}

		if len(directory) > 0 && directory[0] == '@' {
			directory = splitDirectory(directory[1:])
		}
		newParts = append(newParts, directory)
	}

	completePath := strings.Join(newParts, "/")
	return opath.NewFromString(completePath)
}

func NewFromOPath(in opath.OPath) (out OFilePath, err error) {
	fixedPath, conversionErr := splitIntoFileSystemFriendlyPaths(in)
	if conversionErr != nil {
		return OFilePath{}, conversionErr
	}
	path := OFilePath{path: fixedPath}
	return path, nil
}

func (in OFilePath) ToString() string {
	return in.path.ToString()
}

func (in OFilePath) String() string {
	return fmt.Sprintf("[ofilepath %s]", in.path)
}
