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
		src := internal.Cmd.Lookup(internal.Src).Value
		fmt.Printf("Remove rpc.\nSrc path %s\n",
			internal.Cmd.Lookup("src").Value)
		err := internal.Remove(src.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "list":
		fmt.Printf("List rpc.\nRemote path %s\nLocal path %s",
			internal.Cmd.Lookup("des").Value)
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
	default:
		internal.ShowUsage(internal.Subcommands)
	}
}
