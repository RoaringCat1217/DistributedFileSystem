package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"sync"
)

type StorageServer struct {
	clientPort       int
	commandPort      int
	registrationPort int
	service          *gin.Engine
	command          *gin.Engine
	mutex            sync.RWMutex
	fileSystem       *FileSystem
}

func NewStorageServer(directory string, clientPort int, commandPort int, registrationPort int) *StorageServer {
	storageServer := &StorageServer{
		clientPort:       clientPort,
		commandPort:      commandPort,
		registrationPort: registrationPort,
		service:          gin.Default(),
		command:          gin.Default(),
		fileSystem:       &FileSystem{directory},
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
	log.Printf("Trying to register at port %d\n", s.registrationPort)
	for {
		err := s.register()
		if err != nil {
			// log.Printf("Failed to register: %s\n", err.Error())
			continue
		} else {
			log.Println("Registered successfully")
			break
		}
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

	err := <-chanErr
	log.Printf(err.Error())
}

// handleRead handles the HTTP request for reading data from a file.
func (s *StorageServer) handleRead(request ReadRequest) (int, any) {
	data, err := s.fileSystem.ReadFile(request.Path, int64(request.Offset), int64(request.Length))
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, ReadResponse{string(data)}
}

// handleWrite handles the HTTP request for writing data to a file.
func (s *StorageServer) handleWrite(request WriteRequest) (int, any) {
	err := s.fileSystem.WriteFile(request.Path, []byte(request.Data), request.Offset)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, SuccessResponse{true}
}

// handleSize handles the HTTP request for retrieving the size of a file.
func (s *StorageServer) handleSize(request SizeRequest) (int, any) {
	size, err := s.fileSystem.GetFileSize(request.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, SizeResponse{size}
}

// handleCreate handles the HTTP request for creating a new file.
func (s *StorageServer) handleCreate(request CreateRequest) (int, any) {
	success, err := s.fileSystem.CreateFile(request.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, SuccessResponse{success}
}

// handleDelete handles the HTTP request for deleting a file.
func (s *StorageServer) handleDelete(request DeleteRequest) (int, any) {
	success, err := s.fileSystem.DeleteFile(request.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, SuccessResponse{success}
}

// handleCopy handles the HTTP request for copying a file from another storage server.
func (s *StorageServer) handleCopy(request CopyRequest) (int, any) {
	/* ignore it for this checkpoint
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
	*/
	return 0, nil
}

func (s *StorageServer) register() error {
	files, err := s.fileSystem.ListFiles()
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

	url := fmt.Sprintf("http://localhost:%d/register", s.registrationPort)
	log.Printf("Sending registration request to %s\n", url)
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		log.Printf("Failed to send registration request: %v", err)
		return err
	}
	defer resp.Body.Close()

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

	if len(response.Files) > 0 {
		log.Printf("Registration successful. Deleting files: %v", response.Files)

		// Delete files specified by the naming server
		err = s.fileSystem.DeleteFiles(response.Files)
		if err != nil {
			log.Printf("Failed to delete files: %v", err)
			return err
		}

		// Prune empty directories recursively
		err = s.fileSystem.Prune()
		if err != nil {
			log.Printf("Failed to prune empty directories: %v", err)
			return err
		}
	}

	log.Println("Registration completed successfully")
	return nil
}
