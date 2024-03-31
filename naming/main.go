package main

import (
	"fmt"
	naming "naming/lib"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Wrong number of arguments")
		os.Exit(-1)
	}
	servicePort, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("%s is not a valid port number\n", os.Args[1])
		os.Exit(-1)
	}
	registrationPort, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Printf("%s is not a valid port number\n", os.Args[2])
		os.Exit(-1)
	}
	server := naming.NewNamingServer(servicePort, registrationPort)
	server.Run()
}
