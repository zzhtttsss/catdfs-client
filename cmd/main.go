package main

import (
	"fmt"
	"tinydfs-client/internal"
)

func main() {
	switch internal.Cmd.Name() {
	case "get":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		err := internal.Get(src.String(), des.String())
		if err != nil {
			internal.Logger.Errorf("Fail to get a file, src: %s, des: %s, error detail: %s", src, des, err.Error())
			fmt.Println(err.Error())
		}
	case "add":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		err := internal.Add(src.String(), des.String())
		if err != nil {
			internal.Logger.Errorf("Fail to add a file, src: %s, des: %s, error detail: %s", src, des, err.Error())
			fmt.Println(err.Error())
		}

	case "mkdir":
		des := internal.Cmd.Lookup(internal.Des).Value
		err := internal.Mkdir(des.String())
		if err != nil {
			internal.Logger.Errorf("Fail to create a directory, des: %s, error detail: %s", des, err.Error())
			fmt.Println(err.Error())
		}
	case "remove":
		des := internal.Cmd.Lookup(internal.Des).Value
		err := internal.Remove(des.String())
		if err != nil {
			internal.Logger.Errorf("Fail to remove a directory or file, des: %s, error detail: %s", des, err.Error())
			fmt.Println(err.Error())
		}
	case "list":
		des := internal.Cmd.Lookup(internal.Des).Value
		mode := internal.Cmd.Lookup(internal.Mode).Value
		err := internal.List(des.String(), mode.String())
		if err != nil {
			internal.Logger.Errorf("Fail to list a directory, des: %s, mode: %s, error detail: %s", des, mode, err.Error())
			fmt.Println(err.Error())
		}
	case "move":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		err := internal.Move(src.String(), des.String())
		if err != nil {
			internal.Logger.Errorf("Fail to move a directory or file, src: %s, des: %s, error detail: %s", src, des, err.Error())
			fmt.Println(err.Error())
		}
	case "rename":
		src := internal.Cmd.Lookup(internal.Src).Value
		des := internal.Cmd.Lookup(internal.Des).Value
		err := internal.Rename(src.String(), des.String())
		if err != nil {
			internal.Logger.Errorf("Fail to rename a directory or file, src: %s, des: %s, error detail: %s", src, des, err.Error())
			fmt.Println(err.Error())
		}
	case "stat":
		des := internal.Cmd.Lookup(internal.Des).Value
		mode := internal.Cmd.Lookup(internal.Mode).Value
		err := internal.Stat(des.String(), mode.String())
		if err != nil {
			internal.Logger.Errorf("Fail to get status of a directory or file, des: %s, mode: %s, error detail: %s", des, mode, err.Error())
			fmt.Println(err.Error())
		}
	default:
		internal.ShowUsage(internal.Subcommands)
	}
}
