package internal

import (
	"fmt"
	"tinydfs-base/protocol/pb"
)

func List(des string, mode string) error {
	Logger.Infof("Start to list a directory, des: %s, mode: %s", des, mode)
	if des[0] != '/' || des[len(des)-1] != '/' {
		return fmt.Errorf("get the wrong path: %s", des)
	}
	if mode != "-l" && mode != "-s" {
		return fmt.Errorf("get the wrong mode: %s", mode)
	}
	var isLatest = false
	if mode == "-l" {
		isLatest = true
	}
	checkAndListArgs := &pb.CheckAndListArgs{Path: des, IsLatest: isLatest}
	checkAndListReply, err := GlobalClientHandler.CheckAndList(checkAndListArgs)
	if err != nil {
		Logger.Errorf("Fail to list the direcotry, error detail : %s", err)
		return err
	}
	infos := checkAndListReply.Files
	for _, info := range infos {
		if info.IsFile {
			fmt.Printf("[F]\t%s\n", info.FileName)
		} else {
			fmt.Printf("[D]\t%s\n", info.FileName)
		}
	}
	Logger.Infof("Success to list a directory, des: %s, mode: %s", des, mode)
	return nil
}
