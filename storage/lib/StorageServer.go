package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gin-gonic/gin"
)

type StorageServer struct {
	directory   string
	clientPort  int
	commandPort int
	service     *gin.Engine
	command     *gin.Engine
	mutex       sync.RWMutex
	fileSystem  *FileSystem
}

func NewStorageServer(directory string, clientPort int, commandPort int) *StorageServer {
	// Create the storage directory if it doesn't exist
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	storageServer := &StorageServer{
		directory:   directory,
		clientPort:  clientPort,
		commandPort: commandPort,
		service:     gin.Default(),
		command:     gin.Default(),
		fileSystem:  NewFileSystem(directory),
	}

	// Register client APIs
	storageServer.service.POST("/storage_read", func(ctx *gin.Context) {
		var request ReadRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleRead(request)
		ctx.JSON(statusCode, response)
	})
	storageServer.service.POST("/storage_write", func(ctx *gin.Context) {
		var request WriteRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleWrite(request)
		ctx.JSON(statusCode, response)
	})
	storageServer.service.POST("/storage_size", func(ctx *gin.Context) {
		var request SizeRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleSize(request)
		ctx.JSON(statusCode, response)
	})

	// Register command APIs
	storageServer.command.POST("/storage_create", func(ctx *gin.Context) {
		var request CreateRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleCreate(request)
		ctx.JSON(statusCode, response)
	})
	storageServer.command.POST("/storage_delete", func(ctx *gin.Context) {
		var request DeleteRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleDelete(request)
		ctx.JSON(statusCode, response)
	})
	storageServer.command.POST("/storage_copy", func(ctx *gin.Context) {
		var request CopyRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleCopy(request)
		ctx.JSON(statusCode, response)
	})
	return storageServer
}

func (s *StorageServer) Start() {
	err := s.register()
	if err != nil {
		log.Printf("Failed to register: %s\n", err.Error())
	}

	chanErr := make(chan error)
	go func() {
		log.Printf("Storage server client interface listening on port %d\n", s.clientPort)
		err := s.service.Run(fmt.Sprintf("localhost:%d", s.clientPort))
		chanErr <- err
	}()
	go func() {
		log.Printf("Storage server command interface listening on port %d\n", s.commandPort)
		err := s.command.Run(fmt.Sprintf("localhost:%d", s.commandPort))
		chanErr <- err
	}()

	err = <-chanErr
	log.Printf(err.Error())
}

// handleRead handles the HTTP request for reading data from a file.
func (s *StorageServer) handleRead(request ReadRequest) (int, any) {
	data, err := s.fileSystem.ReadFile(request.Path, int64(request.Offset), int64(request.Length))
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	return http.StatusOK, map[string]any{"data": string(data)}
}


// handleWrite handles the HTTP request for writing data to a file.
func (s *StorageServer) handleWrite(request WriteRequest) (int, any) {
	err := s.fileSystem.WriteFile(request.Path, []byte(request.Data), int64(request.Offset))
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	return http.StatusOK, map[string]any{"success": true}
}


// handleSize handles the HTTP request for retrieving the size of a file.
func (s *StorageServer) handleSize(request SizeRequest) (int, any) {
	size, err := s.fileSystem.GetFileSize(request.Path)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	return http.StatusOK, map[string]any{"size": size}
}


// handleCreate handles the HTTP request for creating a new file.
func (s *StorageServer) handleCreate(request CreateRequest) (int, any) {
	err := s.fileSystem.CreateFile(request.Path)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	return http.StatusOK, map[string]any{"success": true}
}


// handleDelete handles the HTTP request for deleting a file.
func (s *StorageServer) handleDelete(request DeleteRequest) (int, any) {
	err := s.fileSystem.DeleteFile(request.Path)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	return http.StatusOK, map[string]any{"success": true}
}


// handleCopy handles the HTTP request for copying a file from another storage server.
func (s *StorageServer) handleCopy(request CopyRequest) (int, any) {
	// Construct the source URL from the request information
	sourceURL := fmt.Sprintf("http://%s:%d/storage_read", request.SourceAddr, request.SourcePort)

	// Create the JSON payload for the POST request
	payload, err := json.Marshal(map[string]any{"path": request.Path, "offset": 0, "length": -1})
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: "Failed to encode request payload"}
	}

	// Execute the POST request to the source server
	resp, err := http.Post(sourceURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: "Failed to fetch data from source server"}
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		var exception DFSException
		if err := json.NewDecoder(resp.Body).Decode(&exception); err != nil {
			return http.StatusInternalServerError, DFSException{Type: IOException, Msg: "Failed to decode error from source server"}
		}
		return resp.StatusCode, exception
	}

	// Read the data from the response
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: "Failed to read data from source server response"}
	}

	// Write the data to a file in the destination directory
	err = s.fileSystem.WriteFile(request.Path, data, 0)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	// Return success response
	return http.StatusOK, SuccessResponse{Success: true}
}


func (s *StorageServer) register() error {
	files, err := s.listFiles()

	if err != nil {
		return err
	}

	reqBody := RegisterRequest{
		StorageIP:   "127.0.0.1",
		ClientPort:  s.clientPort,
		CommandPort: s.commandPort,
		Files:       files,
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	log.Printf("Sending registration request to naming server: localhost")
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:8080"), "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		log.Printf("Failed to send registration request: %v", err)
		return err
	}

	if resp.StatusCode == http.StatusConflict {
		var exception DFSException
		if err := json.NewDecoder(resp.Body).Decode(&exception); err != nil {
			log.Printf("Failed to decode registration response: %s", err.Error())
			return err
		}
		log.Printf("Registration failed: %s", exception.Msg)
		return fmt.Errorf("registration failed: %s", exception.Msg)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Registration failed with status code: %d", resp.StatusCode)
		return fmt.Errorf("registration failed with status code %d", resp.StatusCode)
	}

	var response RegisterResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		log.Printf("Failed to decode registration response: %s", err.Error())
		return err
	}

	log.Printf("Registration successful. Deleting files: %v", response.Files)

	// Delete files that failed to register
	err = s.fileSystem.DeleteFiles(response.Files)
	if err != nil {
		log.Printf("Batch file deletion failed: %v", err)
	}
	log.Println("Registration completed successfully")
	return nil
}

func (s *StorageServer) listFiles() ([]string, error) {
	return s.fileSystem.ListFiles()
}

