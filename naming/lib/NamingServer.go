package naming

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"hash/fnv"
	"io"
	"net/http"
	"path"
	"sync"
)

type StorageServerInfo struct {
	clientPort  int
	commandPort int
}

type NamingServer struct {
	servicePort      int
	registrationPort int
	service          *gin.Engine
	registration     *gin.Engine
	root             *Directory
	// fields that need locking before access
	storageServers []*StorageServerInfo
	lock           sync.RWMutex
}

func NewNamingServer(servicePort int, registrationPort int) *NamingServer {
	namingServer := NamingServer{
		servicePort:      servicePort,
		registrationPort: registrationPort,
		root: &Directory{
			name:   "",
			parent: nil,
		},
		service:      gin.Default(),
		registration: gin.Default(),
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
	namingServer.service.POST("/delete", func(ctx *gin.Context) {
		var request PathRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.deleteHandler(request)
		ctx.JSON(statusCode, response)
	})
	namingServer.service.POST("/create_directory", func(ctx *gin.Context) {
		var request PathRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.createDirectoryHandler(request)
		ctx.JSON(statusCode, response)
	})
	namingServer.service.POST("/create_file", func(ctx *gin.Context) {
		var request PathRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.createFileHandler(request)
		ctx.JSON(statusCode, response)
	})

	// register registration API
	namingServer.registration.POST("/register", func(ctx *gin.Context) {
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

func (s *NamingServer) Run() {
	chanErr := make(chan error)
	go func() {
		err := s.service.Run(fmt.Sprintf("localhost:%d", s.servicePort))
		chanErr <- err
	}()
	go func() {
		err := s.registration.Run(fmt.Sprintf("localhost:%d", s.registrationPort))
		chanErr <- err
	}()

	err := <-chanErr
	fmt.Println(err.Error())
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

	// notify the storage server asynchronously
	for _, file := range deletedFiles {
		go s.storageDeleteCommand(file)
	}
	return http.StatusOK, SuccessResponse{true}
}

func (s *NamingServer) createFileHandler(body PathRequest) (int, any) {
	// allocate a storage server
	s.lock.RLock()
	if len(s.storageServers) == 0 {
		// no storage server
		s.lock.RUnlock()
		err := &DFSException{IllegalStateException, "no storage servers are registered with the naming server."}
		return http.StatusConflict, err
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(path.Clean(body.Path)))
	hash := h.Sum32()
	idx := int(hash % uint32(len(s.storageServers)))
	storageServer := s.storageServers[idx]
	s.lock.RUnlock()

	_, err := s.root.CreateFile(body.Path, storageServer)
	if err != nil {
		return http.StatusNotFound, err
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
			ex := DFSException{IllegalStateException, "This storage server is already registered."}
			return http.StatusConflict, ex
		}
	}
	server := &StorageServerInfo{
		clientPort:  body.ClientPort,
		commandPort: body.CommandPort,
	}
	s.storageServers = append(s.storageServers, server)
	// register all of its files
	success := s.root.RegisterFiles(body.Files, server)
	response := make(map[string][]string)
	response["files"] = make([]string, 0)
	for i := range success {
		if !success[i] {
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
