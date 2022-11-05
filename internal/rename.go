package internal

import (
	"fmt"
	"tinydfs-base/protocol/pb"
)

func Rename(src, des string) error {
	Logger.Infof("Start to rename a directory or file, src: %s, des: %s", src, des)
	if src[0] != '/' || src[len(src)-1] == '/' {
		return fmt.Errorf("get the wrong src: %s", src)
	}
	if des[0] == '/' || des[len(des)-1] == '/' {
		return fmt.Errorf("get the wrong name: %s", des)
	}
	checkAndRenameArgs := &pb.CheckAndRenameArgs{
		Path:    src,
		NewName: des,
	}
	_, err := GlobalClientHandler.CheckAndRename(checkAndRenameArgs)
	if err != nil {
		return err
	}
	Logger.Infof("Success to rename a directory or file, src: %s, des: %s", src, des)
	return nil

}
