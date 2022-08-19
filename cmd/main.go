package main

import "fmt"

func main() {
	switch cmd.Name() {
	case "get":
		fmt.Printf("Get rpc.\nRemote path %s\nLocal path %s",
			cmd.Lookup("src").Value,
			cmd.Lookup("des").Value)
	case "add":
		fmt.Printf("Add rpc.\nRemote path %s\nLocal path %s",
			cmd.Lookup("src").Value,
			cmd.Lookup("des").Value)
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
