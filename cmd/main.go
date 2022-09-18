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
			internal.Cmd.Lookup("src").Value,
			internal.Cmd.Lookup("des").Value)
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
		fmt.Printf("Remove rpc.\nRemote path %s\nLocal path %s",
			internal.Cmd.Lookup("des").Value)
	case "list":
		fmt.Printf("List rpc.\nRemote path %s\nLocal path %s",
			internal.Cmd.Lookup("des").Value)
	case "move":
		fmt.Printf("Move rpc.\nRemote path %s\nLocal path %s",
			internal.Cmd.Lookup("src").Value,
			internal.Cmd.Lookup("des").Value)
	default:
		internal.ShowUsage(internal.Subcommands)
	}
}
