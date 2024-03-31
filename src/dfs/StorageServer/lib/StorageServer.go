package StorageServer

import (
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

	// Register APIs
	storageServer.service.POST("/storage_read", storageServer.handleRead)
	storageServer.service.POST("/storage_write", storageServer.handleWrite)
	storageServer.service.POST("/storage_delete", storageServer.handleDelete)
	storageServer.service.POST("/storage_create", storageServer.handleCreate)
	storageServer.service.POST("/storage_copy", storageServer.handleCopy)
	storageServer.service.POST("/storage_size", storageServer.handleSize)

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
func (s *StorageServer) handleRead(ctx *gin.Context) {
	var request ReadRequest
	if err := ctx.BindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, DFSException{Type: IllegalArgumentException})
		return
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()
	filePath := filepath.Join(s.directory, request.Path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		ctx.JSON(http.StatusNotFound, map[string]any{"success": false, "error": "file not found"})
		return
	}
	if fileInfo.IsDir() {
		ctx.JSON(http.StatusBadRequest, map[string]any{"success": false, "error": "cannot read a directory"})
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, DFSException{Type: "IOException"})
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	_, err = file.Seek(int64(request.Offset), io.SeekStart)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, DFSException{Type: "IOException"})
		return
	}

	data := make([]byte, int(request.Length))
	_, err = file.Read(data)
	if err != nil && err != io.EOF {
		ctx.JSON(http.StatusInternalServerError, DFSException{Type: "IOException"})
		return
	}

	ctx.JSON(http.StatusOK, map[string]any{"data": string(data)})
}

// handleWrite handles the HTTP request for writing data to a file.
func (s *StorageServer) handleWrite(ctx *gin.Context) {
	var request WriteRequest
	if err := ctx.BindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, DFSException{Type: IllegalArgumentException})
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	filePath := filepath.Join(s.directory, request.Path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		ctx.JSON(http.StatusNotFound, map[string]any{"success": false, "error": "file not found"})
		return
	}
	if fileInfo.IsDir() {
		ctx.JSON(http.StatusBadRequest, map[string]any{"success": false, "error": "cannot write to a directory"})
		return
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, DFSException{Type: "IOException"})
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	_, err = file.Seek(int64(request.Offset), io.SeekStart)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, DFSException{Type: "IOException"})
		return
	}

	_, err = file.WriteString(request.Data)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, DFSException{Type: "IOException"})
		return
	}

	ctx.JSON(http.StatusOK, map[string]any{"success": true})
}

// handleDelete handles the HTTP request for deleting a file.
func (s *StorageServer) handleDelete(ctx *gin.Context) {
	var request DeleteRequest
	if err := ctx.BindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, DFSException{Type: IllegalArgumentException})
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	filePath := filepath.Join(s.directory, request.Path)
	err := os.RemoveAll(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			ctx.JSON(http.StatusNotFound, map[string]any{"success": false})
		} else {
			ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false})
		}
		return
	}

	// Remove empty parent directories recursively
	parentDir := filepath.Dir(filePath)
	err = s.pruneEmptyDirs(parentDir)
	if err != nil {
		log.Printf("Failed to prune empty directories: %v", err)
	}

	ctx.JSON(http.StatusOK, map[string]any{"success": true})
}

// handleCreate handles the HTTP request for creating a new file.
func (s *StorageServer) handleCreate(ctx *gin.Context) {
	var request CreateRequest
	if err := ctx.BindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, map[string]any{"success": false})
		return
	}

	if request.Path == "/" {
		ctx.JSON(http.StatusBadRequest, map[string]any{"success": false})
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	filePath := filepath.Join(s.directory, request.Path)
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false, "error": "failed to create parent directories"})
		return
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false})
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	ctx.JSON(http.StatusOK, map[string]any{"success": true})
}

// handleCopy handles the HTTP request for copying a file from another storage server.
func (s *StorageServer) handleCopy(ctx *gin.Context) {
	var request CopyRequest
	if err := ctx.BindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, map[string]any{"success": false})
		return
	}

	sourceURL := fmt.Sprintf("http://%s:%d/storage_read", request.SourceAddr, int(request.SourcePort))
	resp, err := http.Post(sourceURL, "application/json", strings.NewReader(fmt.Sprintf(`{"path":"%s","offset":0,"length":-1}`, request.Path)))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false})
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		ctx.JSON(resp.StatusCode, map[string]any{"success": false})
		return
	}

	var readResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&readResp); err != nil {
		ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false})
		return
	}

	data, ok := readResp["data"].(string)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false})
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	filePath := filepath.Join(s.directory, request.Path)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false})
		return
	}

	if err := os.WriteFile(filePath, []byte(data), os.ModePerm); err != nil {
		ctx.JSON(http.StatusInternalServerError, map[string]any{"success": false})
		return
	}

	ctx.JSON(http.StatusOK, map[string]any{"success": true})
}

// handleSize handles the HTTP request for retrieving the size of a file.
func (s *StorageServer) handleSize(ctx *gin.Context) {
	var request SizeRequest
	if err := ctx.BindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, DFSException{Type: IllegalArgumentException})
		return
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()
	filePath := filepath.Join(s.directory, request.Path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		ctx.JSON(http.StatusNotFound, DFSException{Type: "FileNotFoundException"})
		return
	}
	if fileInfo.IsDir() {
		ctx.JSON(http.StatusBadRequest, DFSException{Type: "FileNotFoundException"})
		return
	}

	size := fileInfo.Size()
	ctx.JSON(http.StatusOK, map[string]any{"size": size})
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

	resp, err := http.Post(fmt.Sprintf("http://%s/register", s.namingServer), "application/json", strings.NewReader(string(reqBytes)))
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(resp.Body)

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
