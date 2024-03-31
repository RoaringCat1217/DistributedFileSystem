package NamingServer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"sync"
)

type StorageServerInfo struct {
	clientPort  int
	commandPort int
}

type NamingServer struct {
	port    int
	service *gin.Engine
	root    *Directory
	// fields that need locking before access
	files          map[*FileInfo]bool
	storageServers []*StorageServerInfo
	lock           sync.RWMutex
}

func NewNamingServer(port int) *NamingServer {
	namingServer := NamingServer{
		port: port,
		root: &Directory{
			name:   "",
			parent: nil,
		},
		service: gin.Default(),
	}

	// register client APIs
	namingServer.service.POST("/is_valid_path", func(ctx *gin.Context) {
		var request PathRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.isValidPathHandler(request)
		ctx.JSON(statusCode, response)
	})
	namingServer.service.POST("/get_storage", func(ctx *gin.Context) {
		var request PathRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.getStorageHandler(request)
		ctx.JSON(statusCode, response)
	})

	// register registration API
	namingServer.service.POST("/register", func(ctx *gin.Context) {
		var request RegisterRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.registerStorageHandler(request)
		ctx.JSON(statusCode, response)
	})
	return &namingServer
}

// handlers for client APIs
func (s *NamingServer) isValidPathHandler(body PathRequest) (int, any) {
	success := s.root.PathExists(body.Path)
	return http.StatusOK, SuccessResponse{success}
}

func (s *NamingServer) getStorageHandler(body PathRequest) (int, any) {
	storageServer, err := s.root.GetFileStorage(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, StorageInfoResponse{"localhost", storageServer.clientPort}
}

func (s *NamingServer) createDirectoryHandler(body PathRequest) (int, any) {
	err := s.root.MakeDirectory(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, SuccessResponse{true}
}

func (s *NamingServer) deleteHandler(body PathRequest) (int, any) {
	deletedFiles, err := s.root.DeletePath(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	if len(deletedFiles) == 0 {
		return http.StatusOK, SuccessResponse{false}
	}

	// modify NamingServer.files
	s.lock.Lock()
	for _, file := range deletedFiles {
		delete(s.files, file)
		// notify the storage server asynchronously
		go s.storageDeleteCommand(file)
	}
	return http.StatusOK, SuccessResponse{true}
}

// handler for registration API
func (s *NamingServer) registerStorageHandler(body RegisterRequest) (int, any) {
	// check if this storage server is already registered
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, server := range s.storageServers {
		if server.clientPort == body.ClientPort && server.commandPort == body.CommandPort {
			// already registered
			ex := DFSException{IllegalArgumentException, "This storage server is already registered."}
			return http.StatusConflict, ex
		}
	}
	server := &StorageServerInfo{
		clientPort:  body.ClientPort,
		commandPort: body.CommandPort,
	}
	s.storageServers = append(s.storageServers, server)
	// create new files
	files := make([]*FileInfo, 0)
	for _, fileName := range body.Files {
		file := FileInfo{
			name:          fileName,
			parent:        nil,
			storageServer: server,
		}
		files = append(files, &file)
	}
	// register all of its files
	success := s.root.RegisterFiles(body.Files, files)
	response := make(map[string][]string)
	response["files"] = make([]string, 0)
	for i := range success {
		if success[i] {
			s.files[files[i]] = true
		} else {
			// delete files that fail to register
			response["files"] = append(response["files"], body.Files[i])
		}
	}
	return http.StatusOK, response
}

// commands for storage servers
func (s *NamingServer) storageDeleteCommand(file *FileInfo) {
	url := fmt.Sprintf("http://localhost:%d/storage_delete", file.storageServer.commandPort)
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"path":"%s"}`, file.path)))
	resp, err := http.Post(url, "application/json", body)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	var success SuccessResponse
	err = json.Unmarshal(data, &success)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if !success.Success {
		fmt.Printf("storage_delete failed for file %s (storage server %v)\n", file.path, file.storageServer)
		return
	}
}
