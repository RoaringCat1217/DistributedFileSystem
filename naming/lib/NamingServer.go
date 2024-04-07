package naming

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
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
			name:         "",
			parent:       nil,
			lock:         NewFIFORWMutex(),
			rLockedItems: make(map[string]*RLockedItem),
			wLockedItems: make(map[string]FSItem),
		},
		service:      gin.Default(),
		registration: gin.Default(),
	}
	namingServer.root.namingServer = &namingServer

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
	namingServer.service.POST("/list", func(ctx *gin.Context) {
		var request PathRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.listDirHandler(request)
		ctx.JSON(statusCode, response)
	})
	namingServer.service.POST("/is_directory", func(ctx *gin.Context) {
		var request PathRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.isDirectoryHandler(request)
		ctx.JSON(statusCode, response)
	})
	namingServer.service.POST("/lock", func(ctx *gin.Context) {
		var request LockRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.lockHandler(request)
		if response != nil {
			ctx.JSON(statusCode, response)
		} else {
			ctx.Status(statusCode)
		}
	})
	namingServer.service.POST("/unlock", func(ctx *gin.Context) {
		var request LockRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := namingServer.unlockHandler(request)
		if response != nil {
			ctx.JSON(statusCode, response)
		} else {
			ctx.Status(statusCode)
		}
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
