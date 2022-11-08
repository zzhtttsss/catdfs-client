package internal

import (
	"tinydfs-base/protocol/pb"
)

func Remove(des string) error {
	Logger.Infof("Start to remove a directory or file, des: %s", des)
	checkAndRemoveArgs := &pb.CheckAndRemoveArgs{
		Path: des,
	}
	_, err := GlobalClientHandler.CheckAndRemove(checkAndRemoveArgs)
	if err != nil {
		return err
	}
	Logger.Infof("Success to remove a directory or file des: %s", des)
	return nil
}
