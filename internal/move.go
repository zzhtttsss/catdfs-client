package internal

import (
	"github.com/sirupsen/logrus"
	"tinydfs-base/protocol/pb"
)

func Move(src string, des string) error {
	checkAndMoveArgs := &pb.CheckAndMoveArgs{
		SourcePath: src,
		TargetPath: des,
	}
	_, err := GlobalClientHandler.CheckAndMove(checkAndMoveArgs)
	if err != nil {
		logrus.Errorf("fail to check args and move directory or file to target path, error detail: %s", err.Error())
		return err
	}
	return nil
}
