package internal

import (
	"tinydfs-base/protocol/pb"
)

func Move(src string, des string) error {
	Logger.Infof("Start to move a directory or file, src: %s, des: %s", src, des)
	checkAndMoveArgs := &pb.CheckAndMoveArgs{
		SourcePath: src,
		TargetPath: des,
	}
	_, err := GlobalClientHandler.CheckAndMove(checkAndMoveArgs)
	if err != nil {
		return err
	}
	Logger.Infof("Success to move a directory or file, src: %s, des: %s", src, des)
	return nil
}
