package internal

import (
	"github.com/sirupsen/logrus"
	"tinydfs-base/protocol/pb"
)

func Remove(src string) error {
	checkAndRemoveArgs := &pb.CheckAndRemoveArgs{
		Path: src,
	}
	_, err := GlobalClientHandler.CheckAndRemove(checkAndRemoveArgs)
	if err != nil {
		logrus.Errorf("fail to check args and remove directory or file at target path, error detail: %s", err.Error())
		return err
	}
	return nil
}
