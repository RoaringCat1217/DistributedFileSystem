package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

type StorageServer struct {
	directory    string
	namingServer string
	clientPort   int
	commandPort  int
	service      *gin.Engine
	mutex        sync.RWMutex
}

func NewStorageServer(directory string, namingServer string, clientPort int, commandPort int) *StorageServer {
	storageServer := &StorageServer{
		directory:    directory,
		namingServer: namingServer,
		clientPort:   clientPort,
		commandPort:  commandPort,
		service:      gin.Default(),
	}

	// Register client APIs
	storageServer.service.POST("/storage_read", func(ctx *gin.Context) {
		var request ReadRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleRead(ctx, request)
		ctx.JSON(statusCode, response)
	})
	storageServer.service.POST("/storage_write", func(ctx *gin.Context) {
		var request WriteRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleWrite(ctx, request)
		ctx.JSON(statusCode, response)
	})
	storageServer.service.POST("/storage_size", func(ctx *gin.Context) {
		var request SizeRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleSize(ctx, request)
		ctx.JSON(statusCode, response)
	})

	// Register command APIs
	storageServer.service.POST("/storage_create", func(ctx *gin.Context) {
		var request CreateRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleCreate(ctx, request)
		ctx.JSON(statusCode, response)
	})
	storageServer.service.POST("/storage_delete", func(ctx *gin.Context) {
		var request DeleteRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleDelete(ctx, request)
		ctx.JSON(statusCode, response)
	})
	storageServer.service.POST("/storage_copy", func(ctx *gin.Context) {
		var request CopyRequest
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, nil)
			return
		}
		statusCode, response := storageServer.handleCopy(ctx, request)
		ctx.JSON(statusCode, response)
	})

	return storageServer
}

func (s *StorageServer) Start() {
	go func() {
		log.Printf("Storage server client interface listening on port %d\n", s.clientPort)
		err := s.service.Run(fmt.Sprintf(":%d", s.clientPort))
		if err != nil {
			return
		}
	}()
	go func() {
		log.Printf("Storage server command interface listening on port %d\n", s.commandPort)
		err := s.service.Run(fmt.Sprintf(":%d", s.commandPort))
		if err != nil {
			return
		}
	}()

	if err := s.register(); err != nil {
		log.Fatalf("Failed to register with the naming server: %v", err)
	}
}

func (s *StorageServer) pruneEmptyDirs(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subdir := filepath.Join(dir, entry.Name())
			if err := s.pruneEmptyDirs(subdir); err != nil {
				return err
			}
		}
	}

	entries, err = os.ReadDir(dir)
	if err != nil {
		return err
	}

	if len(entries) == 0 && dir != s.directory {
		if err := os.Remove(dir); err != nil {
			return err
		}
	}

	return nil
}

// handleRead handles the HTTP request for reading data from a file.
func (s *StorageServer) handleRead(ctx *gin.Context, request ReadRequest) (int, any) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	filePath := filepath.Join(s.directory, request.Path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return http.StatusNotFound, DFSException{Type: FileNotFoundException, Msg: "file not found"}
	}
	if fileInfo.IsDir() {
		return http.StatusBadRequest, DFSException{Type: IllegalStateException, Msg: "cannot read a directory"}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	_, err = file.Seek(int64(request.Offset), io.SeekStart)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	data := make([]byte, int(request.Length))
	_, err = file.Read(data)
	if err != nil && err != io.EOF {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	return http.StatusOK, map[string]any{"data": string(data)}
}

// handleWrite handles the HTTP request for writing data to a file.
func (s *StorageServer) handleWrite(ctx *gin.Context, request WriteRequest) (int, any) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	filePath := filepath.Join(s.directory, request.Path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return http.StatusNotFound, DFSException{Type: FileNotFoundException, Msg: "file not found"}
	}
	if fileInfo.IsDir() {
		return http.StatusBadRequest, DFSException{Type: IllegalStateException, Msg: "cannot write to a directory"}
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	_, err = file.Seek(int64(request.Offset), io.SeekStart)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	_, err = file.WriteString(request.Data)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	return http.StatusOK, map[string]any{"success": true}
}

// handleSize handles the HTTP request for retrieving the size of a file.
func (s *StorageServer) handleSize(ctx *gin.Context, request SizeRequest) (int, any) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	filePath := filepath.Join(s.directory, request.Path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return http.StatusNotFound, DFSException{Type: FileNotFoundException}
	}
	if fileInfo.IsDir() {
		return http.StatusBadRequest, DFSException{Type: IllegalStateException, Msg: "cannot get size of a directory"}
	}

	size := fileInfo.Size()
	return http.StatusOK, map[string]any{"size": size}
}

// handleCreate handles the HTTP request for creating a new file.
func (s *StorageServer) handleCreate(ctx *gin.Context, request CreateRequest) (int, any) {
	if request.Path == "/" {
		return http.StatusBadRequest, DFSException{Type: IllegalArgumentException, Msg: "Path is invalid"}
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	filePath := filepath.Join(s.directory, request.Path)
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	return http.StatusOK, map[string]any{"success": true}
}

// handleDelete handles the HTTP request for deleting a file.
func (s *StorageServer) handleDelete(ctx *gin.Context, request DeleteRequest) (int, any) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	filePath := filepath.Join(s.directory, request.Path)
	err := os.RemoveAll(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return http.StatusNotFound, DFSException{Type: FileNotFoundException}
		} else {
			return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
		}
	}

	// Remove empty parent directories recursively
	parentDir := filepath.Dir(filePath)
	err = s.pruneEmptyDirs(parentDir)
	if err != nil {
		log.Printf("Failed to prune empty directories: %v", err)
	}

	return http.StatusOK, map[string]any{"success": true}
}

// handleCopy handles the HTTP request for copying a file from another storage server.
func (s *StorageServer) handleCopy(ctx *gin.Context, request CopyRequest) (int, any) {
	sourceURL := fmt.Sprintf("http://%s:%d/storage_read", request.SourceAddr, int(request.SourcePort))
	resp, err := http.Post(sourceURL, "application/json", strings.NewReader(fmt.Sprintf(`{"path":"%s","offset":0,"length":-1}`, request.Path)))
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		var exception DFSException
		if err := json.NewDecoder(resp.Body).Decode(&exception); err != nil {
			return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
		}
		return resp.StatusCode, exception
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	filePath := filepath.Join(s.directory, request.Path)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	if err := os.WriteFile(filePath, data, os.ModePerm); err != nil {
		return http.StatusInternalServerError, DFSException{Type: IOException, Msg: err.Error()}
	}

	return http.StatusOK, map[string]any{"success": true}
}

func (s *StorageServer) register() error {
	s.mutex.RLock()
	files, err := s.listFiles()
	s.mutex.RUnlock()

	if err != nil {
		return err
	}

	reqBody := map[string]any{
		"storage_ip":   "127.0.0.1",
		"client_port":  s.clientPort,
		"command_port": s.commandPort,
		"files":        files,
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/register", s.namingServer), "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode == http.StatusConflict {
		var exception DFSException
		if err := json.NewDecoder(resp.Body).Decode(&exception); err != nil {
			return err
		}
		return fmt.Errorf("registration failed: %s", exception.Msg)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registration failed with status code %d", resp.StatusCode)
	}

	var filesReturn struct {
		Files []string `json:"files"`
	}
	err = json.NewDecoder(resp.Body).Decode(&filesReturn)
	if err != nil {
		return err
	}

	for _, file := range filesReturn.Files {
		filePath := filepath.Join(s.directory, file)
		err = os.RemoveAll(filePath)
		if err != nil {
			log.Printf("Failed to remove file %s: %v", file, err)
		}
	}

	err = s.pruneEmptyDirs(s.directory)
	if err != nil {
		log.Printf("Failed to prune empty directories: %v", err)
	}

	return nil
}

func (s *StorageServer) listFiles() ([]string, error) {
	var files []string
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	err := filepath.Walk(s.directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(s.directory, path)
			if err != nil {
				return err
			}
			files = append(files, "/"+relPath)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
