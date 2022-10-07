package internal

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strings"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

func Mkdir(des string) error {
	var (
		dirName    string
		targetPath string
	)
	addr := viper.GetString(common.MasterPort)
	logrus.Infof("finish get etcd client, end point is %s", addr)
	logrus.Infof("1")
	des = strings.TrimRight(des, pathSplitString)
	desPath := strings.Split(des, pathSplitString)
	if desPath[0] != "" {
		return fmt.Errorf("Get the wrong path: %s\n", des)
	}
	logrus.Infof("2")
	desPathLength := len(desPath)

	dirName = desPath[desPathLength-1]
	targetPath = strings.Join(desPath[:desPathLength-1], pathSplitString)
	logrus.Infof("3")
	checkAndMkDirArgs := &pb.CheckAndMkDirArgs{
		Path:    targetPath,
		DirName: dirName,
	}
	_, err := GlobalClientHandler.CheckAndMkdir(checkAndMkDirArgs)
	logrus.Infof("4")

	if err != nil {
		logrus.Errorf("fail to check args and make directory at target path, error detail: %s", err.Error())
		return err
	}
	return nil
}
