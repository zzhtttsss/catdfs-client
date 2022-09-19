package internal

import (
	"github.com/sirupsen/logrus"
	"tinydfs-base/protocol/pb"
)

func List(directory string) error {
	checkAndListArgs := &pb.CheckAndListArgs{Path: directory}
	checkAndListReply, err := GlobalClientHandler.CheckAndList(checkAndListArgs)
	if err != nil {
		logrus.Errorf("fail to list the direcotry. Error detail : %s", err)
		return err
	}
	infos := checkAndListReply.Files
	for _, info := range infos {
		if info.IsFile {
			logrus.Printf("[F]\t%s\n", info.FileName)
		} else {
			logrus.Printf("[D]\t%s\n", info.FileName)
		}
	}
	return nil
}
