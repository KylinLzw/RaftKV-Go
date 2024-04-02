package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		text := scanner.Scan()
		if len(text) == 0 {
			continue
		}

		command := strings.Fields(string(text))
		if len(command) == 0 {
			continue
		}

		request := Request{
			Type: command[0],
		}
		switch request.Type {
		case "get":
			if len(command) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			request.Key = command[1]
		case "put":
			if len(command) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			request.Key, request.Value = command[1], command[2]
		case "query":
			request.Type = "QueryShardCtrler"
		default:
			fmt.Println("Unknown command:", command[0])
			continue
		}

		jsonData, err := json.Marshal(request)
		if err != nil {
			fmt.Println("Error marshalling request:", err)
			continue
		}

		// TODO: Send jsonData to the server and handle the response.
		fmt.Println("Sent request:", string(jsonData))
	}
}

// Request represents a client request.
type Request struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}
