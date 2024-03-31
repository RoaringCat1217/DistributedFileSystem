package main

import (
	"fmt"
	"log"
	"os"
	storage "storage/lib"
	"strconv"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Println("Wrong number of arguments")
		os.Exit(-1)
	}

	directory := os.Args[1]
	namingServer := os.Args[2]

	clientPort, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Printf("%s is not a valid port number\n", os.Args[3])
		os.Exit(-1)
	}

	commandPort, err := strconv.Atoi(os.Args[4])
	if err != nil {
		fmt.Printf("%s is not a valid port number\n", os.Args[4])
		os.Exit(-1)
	}

	// Ensure the storage directory exists
	err = os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	server := storage.NewStorageServer(directory, namingServer, clientPort, commandPort)
	server.Start()
}
