package internal

import (
	"github.com/sirupsen/logrus"
	"tinydfs-base/protocol/pb"
)

func Stat(des string) error {
	checkAndStatArgs := &pb.CheckAndStatArgs{Path: des}
	checkAndStatReply, err := GlobalClientHandler.CheckAndStat(checkAndStatArgs)
	if err != nil {
		logrus.Errorf("fail to get the info of file. Error Detail %s", err)
		return err
	}
	if checkAndStatReply.IsFile {
		logrus.Infof("FileName: %s. Size: %d", checkAndStatReply.FileName, checkAndStatReply.Size)
	} else {
		logrus.Infof("Directory: %s.", checkAndStatReply.FileName)
	}
	return nil
}
