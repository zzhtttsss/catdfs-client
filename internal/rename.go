package internal

import (
	"fmt"
	"tinydfs-base/protocol/pb"
)

func Rename(path, newName string) error {
	if path[0] != '/' || path[len(path)-1] == '/' {
		return fmt.Errorf("Get the wrong path: %s\n", path)
	}
	if newName[0] == '/' || newName[len(newName)-1] == '/' {
		return fmt.Errorf("Get the wrong name: %s\n", newName)
	}
	checkAndRenameArgs := &pb.CheckAndRenameArgs{
		Path:    path,
		NewName: newName,
	}
	_, err := GlobalClientHandler.CheckAndRename(checkAndRenameArgs)
	if err != nil {
		return err
	}
	return nil

}
