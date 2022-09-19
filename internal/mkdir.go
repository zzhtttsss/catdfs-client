package internal

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"
	"tinydfs-base/protocol/pb"
)

func Mkdir(des string) error {
	var (
		dirName    string
		targetPath string
	)

	des = strings.TrimRight(des, pathSplitString)
	desPath := strings.Split(des, pathSplitString)
	if desPath[0] != "" {
		return fmt.Errorf("Get the wrong path: %s\n", des)
	}
	desPathLength := len(desPath)

	dirName = desPath[desPathLength-1]
	targetPath = strings.Join(desPath[:desPathLength-1], pathSplitString)

	checkAndMkDirArgs := &pb.CheckAndMkDirArgs{
		Path:    targetPath,
		DirName: dirName,
	}
	_, err := GlobalClientHandler.CheckAndMkdir(checkAndMkDirArgs)
	if err != nil {
		logrus.Errorf("fail to check args and make directory at target path, error detail: %s", err.Error())
		return err
	}
	return nil
}
