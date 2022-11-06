package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"tinydfs-client/internal"
)

func main() {
	switch internal.Cmd.Name() {
	case "get":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		logrus.Infof("Start Get operation.Put %s into %s", src, des)
		err := internal.Get(src.String(), des.String())
		if err != nil {
			logrus.Errorf("Get err.Error detail: %s", err)
			_ = os.Remove(des.String())
		}
	case "add":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		fmt.Printf("Add rpc.\nRemote path %s\nLocal path %s\n",
			src,
			des)
		err := internal.Add(src.String(), des.String())
		if err != nil {
			fmt.Println(err.Error())
		}

	case "mkdir":
		des := internal.Cmd.Lookup(internal.Des).Value
		fmt.Printf("Mkdir rpc.\nRemote path %s\n",
			internal.Cmd.Lookup("des").Value)
		err := internal.Mkdir(des.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "remove":
		src := internal.Cmd.Lookup(internal.Des).Value
		fmt.Printf("Remove rpc.\nDes path %s\n",
			internal.Cmd.Lookup("des").Value)
		err := internal.Remove(src.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "list":
		des := internal.Cmd.Lookup(internal.Des).Value
		mode := internal.Cmd.Lookup(internal.Mode).Value
		fmt.Printf("List rpc.\nRemote direcotry %s\n", des)
		err := internal.List(des.String(), mode.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "move":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		fmt.Printf("Move rpc.\nSrc path %s\nDes path %s\n",
			internal.Cmd.Lookup("src").Value,
			internal.Cmd.Lookup("des").Value)
		err := internal.Move(src.String(), des.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "rename":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		fmt.Printf("Rename rpc.\nSrc path %s\nDes path %s\n",
			src,
			des)
		err := internal.Rename(src.String(), des.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "stat":
		des := internal.Cmd.Lookup(internal.Des).Value
		mode := internal.Cmd.Lookup(internal.Mode).Value
		fmt.Printf("Stat rpc.\nRemote path %s\n", des)
		err := internal.Stat(des.String(), mode.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	default:
		internal.ShowUsage(internal.Subcommands)
	}
}
