package main

import (
	"fmt"
	"tinydfs-base/config"
	"tinydfs-client/internal"
)

const (
	SrcString = Src
)

func main() {
	config.InitConfig()
	switch cmd.Name() {
	case "get":
		fmt.Printf("Get rpc.\nRemote path %s\nLocal path %s",
			cmd.Lookup("src").Value,
			cmd.Lookup("des").Value)
	case "add":
		src := cmd.Lookup(Src).Value
		des := cmd.Lookup(Des).Value
		fmt.Printf("Add rpc.\nRemote path %s\nLocal path %s\n",
			cmd.Lookup("src").Value,
			cmd.Lookup("des").Value)
		err := internal.Add(src.String(), des.String())
		if err != nil {
			fmt.Println(err.Error())
		}
	case "remove":
		fmt.Printf("Remove rpc.\nRemote path %s\nLocal path %s",
			cmd.Lookup("des").Value)
	case "list":
		fmt.Printf("List rpc.\nRemote path %s\nLocal path %s",
			cmd.Lookup("des").Value)
	case "move":
		fmt.Printf("Move rpc.\nRemote path %s\nLocal path %s",
			cmd.Lookup("src").Value,
			cmd.Lookup("des").Value)
	default:
		showUsage(subcommands)
	}
}
