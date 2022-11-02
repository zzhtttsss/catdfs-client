package internal

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"tinydfs-base/protocol/pb"
)

func List(directory string, mode string) error {
	if directory[0] != '/' || directory[len(directory)-1] != '/' {
		return fmt.Errorf("Get the wrong path: %s\n", directory)
	}
	if mode != "-l" && mode != "-s" {
		return fmt.Errorf("Get the wrong mode: %s\n", mode)
	}
	var isLatest = false
	if mode == "-l" {
		isLatest = true
	}
	checkAndListArgs := &pb.CheckAndListArgs{Path: directory, IsLatest: isLatest}
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
