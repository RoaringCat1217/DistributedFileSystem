package NamingServer

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"sync"
)

type NamingServer struct {
	port    int
	service *gin.Engine
	root    *Directory
	// fields that need locking before access
	files          []*FileInfo
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

	// register APIs
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
	return &namingServer
}

// handlers for client APIs
func (s *NamingServer) isValidPathHandler(body PathRequest) (int, any) {
	success := s.root.PathExists(body.Path)
	return http.StatusOK, map[string]any{"success": success}
}

func (s *NamingServer) getStorageHandler(body PathRequest) (int, any) {
	storageServer, err := s.root.GetFileStorage(body.Path)
	response := make(map[string]any)
	if err != nil {
		return http.StatusNotFound, err
	}
	response["service_ip"] = "localhost"
	response["server_port"] = storageServer.port
	return http.StatusOK, response
}

func (s *NamingServer) createDirectoryHandler(body PathRequest) (int, any) {
	err := s.root.MakeDirectory(body.Path)
	response := make(map[string]any)
	if err != nil {
		return http.StatusNotFound, err
	}
	response["success"] = true
	return http.StatusOK, response
}

// handlers for registration APIs
func (s *NamingServer) registerHandler(body RegisterRequest) (int, any) {
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
	// register all of its files

}

type StorageServerInfo struct {
	clientPort  int
	commandPort int
}
