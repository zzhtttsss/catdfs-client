package internal

import (
	"fmt"
	"tinydfs-base/protocol/pb"
)

func Stat(des string, mode string) error {
	Logger.Infof("Start to get status of a directory or file, des: %s, mode: %s", des, mode)
	if des[0] != '/' || des[len(des)-1] == '/' {
		return fmt.Errorf("get the wrong path: %s", des)
	}
	if mode != "-l" && mode != "-s" {
		return fmt.Errorf("get the wrong mode: %s", mode)
	}
	var isLatest = false
	if mode == "-l" {
		isLatest = true
	}
	checkAndStatArgs := &pb.CheckAndStatArgs{Path: des, IsLatest: isLatest}
	checkAndStatReply, err := GlobalClientHandler.CheckAndStat(checkAndStatArgs)
	if err != nil {
		Logger.Errorf("Fail to get the info of file. Error Detail %s", err)
		return err
	}
	if checkAndStatReply.IsFile {
		fmt.Printf("FileName: %s    Size: %d.", checkAndStatReply.FileName, checkAndStatReply.Size)
	} else {
		fmt.Printf("Directory: %s.", checkAndStatReply.FileName)
	}
	Logger.Infof("Start to get status of a directory or file, des: %s, mode: %s", des, mode)
	return nil
}
