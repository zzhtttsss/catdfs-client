package main

import (
	"fmt"
	"tinydfs-base/config"
	"tinydfs-client/internal"
)

const (
	SrcString = internal.Src
)

func main() {
	config.InitConfig()
	switch internal.Cmd.Name() {
	case "get":
		fmt.Printf("Get rpc.\nRemote path %s\nLocal path %s",
			internal.Cmd.Lookup("src").Value,
			internal.Cmd.Lookup("des").Value)
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
	case "remove":
		fmt.Printf("Remove rpc.\nRemote path %s\nLocal path %s",
			internal.Cmd.Lookup("des").Value)
	case "list":
		des := internal.Cmd.Lookup(internal.Des).Value
		fmt.Printf("List rpc.\nRemote direcotry %s\n", des)
		err := internal.List(des.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "move":
		fmt.Printf("Move rpc.\nRemote path %s\nLocal path %s",
			internal.Cmd.Lookup("src").Value,
			internal.Cmd.Lookup("des").Value)
	case "stat":
		des := internal.Cmd.Lookup(internal.Des).Value
		fmt.Printf("Stat rpc.\nRemote path %s\n", des)
		err := internal.Stat(des.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	default:
		internal.ShowUsage(internal.Subcommands)
	}
}
