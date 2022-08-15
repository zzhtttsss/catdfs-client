package config

import (
	"fmt"
	"github.com/spf13/viper"
)

func Config() {
	viper.SetConfigFile("./config.yaml")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig() // 查找并读取配置文件
	if err != nil {             // 处理读取配置文件的错误
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}
