package main

import (
	"fmt"
	"log"
	"os"
	storage "storage/lib"
	"strconv"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Wrong number of arguments")
		os.Exit(-1)
	}
	clientPort, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("%s is not a valid port number\n", os.Args[1])
		os.Exit(-1)
	}

	commandPort, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("%s is not a valid port number\n", os.Args[2])
		os.Exit(-1)
	}
	directory := os.Args[3]

	// Ensure the storage directory exists
	err = os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	server := storage.NewStorageServer(directory, clientPort, commandPort)
	server.Start()
}
