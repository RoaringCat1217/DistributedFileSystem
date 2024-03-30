package StorageServer

import (
	"encoding/json"
	"fmt"
	"dfs/HttpUtil"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type StorageServer struct {
	directory      string
	namingServer   string
	clientPort     int
	commandPort    int
	httpAPIBuilder *HttpUtils.HttpAPIBuilder
	mutex          sync.Mutex
}

func NewStorageServer(directory string, namingServer string, clientPort int, commandPort int) *StorageServer {
	return &StorageServer{
		directory:      directory,
		namingServer:   namingServer,
		clientPort:     clientPort,
		commandPort:    commandPort,
		httpAPIBuilder: HttpUtils.NewHttpAPIBuilder(),
	}
}

func (s *StorageServer) Start() {
	s.registerHandlers()
	s.httpAPIBuilder.Build()
	go func() {
		log.Printf("Storage server client interface listening on port %d\n", s.clientPort)
		http.ListenAndServe(fmt.Sprintf(":%d", s.clientPort), nil)
	}()
	go func() {
		log.Printf("Storage server command interface listening on port %d\n", s.commandPort)
		http.ListenAndServe(fmt.Sprintf(":%d", s.commandPort), nil)
	}()
}

func (s *StorageServer) registerHandlers() {
	s.httpAPIBuilder.RegisterHandler("/storage_read", "POST", s.handleRead)
	s.httpAPIBuilder.RegisterHandler("/storage_write", "POST", s.handleWrite)
	s.httpAPIBuilder.RegisterHandler("/storage_delete", "POST", s.handleDelete)
	s.httpAPIBuilder.RegisterHandler("/storage_create", "POST", s.handleCreate)
	s.httpAPIBuilder.RegisterHandler("/storage_copy", "POST", s.handleCopy)
	s.httpAPIBuilder.RegisterHandler("/storage_size", "POST", s.handleSize)
}

// isDirEmpty checks if a directory is empty.
func isDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// handleRead handles the HTTP request for reading data from a file.
// It takes the file path, offset, and length as parameters in the request body.
// It returns the read data in the response body.
func (s *StorageServer) handleRead(req map[string]any) (int, map[string]any) {
	path, ok := req["path"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}
	offset, ok := req["offset"].(float64)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}
	length, ok := req["length"].(float64)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}

	filePath := filepath.Join(s.directory, path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return http.StatusNotFound, map[string]any{"exception_type": "FileNotFoundException"}
	}
	if fileInfo.IsDir() {
		return http.StatusBadRequest, map[string]any{"exception_type": "FileNotFoundException"}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return http.StatusInternalServerError, map[string]any{"exception_type": "IOException"}
	}
	defer file.Close()

	_, err = file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return http.StatusInternalServerError, map[string]any{"exception_type": "IOException"}
	}

	data := make([]byte, int(length))
	_, err = file.Read(data)
	if err != nil && err != io.EOF {
		return http.StatusInternalServerError, map[string]any{"exception_type": "IOException"}
	}

	return http.StatusOK, map[string]any{"data": string(data)}
}

// handleWrite handles the HTTP request for writing data to a file.
// It takes the file path, offset, and data as parameters in the request body.
// It writes the data to the specified file at the given offset.
func (s *StorageServer) handleWrite(req map[string]any) (int, map[string]any) {
	path, ok := req["path"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}
	offset, ok := req["offset"].(float64)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}
	data, ok := req["data"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}

	filePath := filepath.Join(s.directory, path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return http.StatusNotFound, map[string]any{"exception_type": "FileNotFoundException"}
	}
	if fileInfo.IsDir() {
		return http.StatusBadRequest, map[string]any{"exception_type": "FileNotFoundException"}
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return http.StatusInternalServerError, map[string]any{"exception_type": "IOException"}
	}
	defer file.Close()

	_, err = file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return http.StatusInternalServerError, map[string]any{"exception_type": "IOException"}
	}

	_, err = file.WriteString(data)
	if err != nil {
		return http.StatusInternalServerError, map[string]any{"exception_type": "IOException"}
	}

	return http.StatusOK, map[string]any{"success": true}
}

// handleDelete handles the HTTP request for deleting a file.
// It takes the file path as a parameter in the request body.
// It deletes the specified file from the storage server.
func (s *StorageServer) handleDelete(req map[string]any) (int, map[string]any) {
	path, ok := req["path"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}

	filePath := filepath.Join(s.directory, path)
	err := os.RemoveAll(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return http.StatusNotFound, map[string]any{"success": false}
		}
		return http.StatusInternalServerError, map[string]any{"success": false}
	}

	// Remove empty parent directories recursively
	parentDir := filepath.Dir(filePath)
	for parentDir != s.directory {
		if isEmpty, _ := isDirEmpty(parentDir); isEmpty {
			os.RemoveAll(parentDir)
			parentDir = filepath.Dir(parentDir)
		} else {
			break
		}
	}

	return http.StatusOK, map[string]any{"success": true}
}

// handleCreate handles the HTTP request for creating a new file.
// It takes the file path as a parameter in the request body.
// It creates a new file at the specified path on the storage server.
func (s *StorageServer) handleCreate(req map[string]any) (int, map[string]any) {
	path, ok := req["path"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"success": false}
	}

	if path == "/" {
		return http.StatusBadRequest, map[string]any{"success": false}
	}

	filePath := filepath.Join(s.directory, path)
	if fileInfo, err := os.Stat(filePath); err == nil {
		if fileInfo.IsDir() {
			return http.StatusBadRequest, map[string]any{"success": false}
		}
		return http.StatusConflict, map[string]any{"success": false}
	}

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return http.StatusInternalServerError, map[string]any{"success": false}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return http.StatusInternalServerError, map[string]any{"success": false}
	}
	defer file.Close()

	return http.StatusOK, map[string]any{"success": true}
}

// handleCopy handles the HTTP request for copying a file from another storage server.
// It takes the file path, source server address, and source server port as parameters in the request body.
// It retrieves the file data from the source server and saves it to the specified path on the current storage server.
func (s *StorageServer) handleCopy(req map[string]any) (int, map[string]any) {
	path, ok := req["path"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"success": false}
	}
	sourceAddr, ok := req["source_addr"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"success": false}
	}
	sourcePort, ok := req["source_port"].(float64)
	if !ok {
		return http.StatusBadRequest, map[string]any{"success": false}
	}

	sourceURL := fmt.Sprintf("http://%s:%d/storage_read", sourceAddr, int(sourcePort))
	resp, err := http.Post(sourceURL, "application/json", strings.NewReader(fmt.Sprintf(`{"path":"%s","offset":0,"length":-1}`, path)))
	if err != nil {
		return http.StatusInternalServerError, map[string]any{"success": false}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, map[string]any{"success": false}
	}

	var readResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&readResp); err != nil {
		return http.StatusInternalServerError, map[string]any{"success": false}
	}

	data, ok := readResp["data"].(string)
	if !ok {
		return http.StatusInternalServerError, map[string]any{"success": false}
	}

	filePath := filepath.Join(s.directory, path)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return http.StatusInternalServerError, map[string]any{"success": false}
	}

	if err := os.WriteFile(filePath, []byte(data), os.ModePerm); err != nil {
		return http.StatusInternalServerError, map[string]any{"success": false}
	}

	return http.StatusOK, map[string]any{"success": true}
}

// handleSize handles the HTTP request for retrieving the size of a file.
// It takes the file path as a parameter in the request body.
// It returns the size of the specified file in the response body.
func (s *StorageServer) handleSize(req map[string]any) (int, map[string]any) {
	path, ok := req["path"].(string)
	if !ok {
		return http.StatusBadRequest, map[string]any{"exception_type": "IllegalArgumentException"}
	}

	filePath := filepath.Join(s.directory, path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return http.StatusNotFound, map[string]any{"exception_type": "FileNotFoundException"}
	}
	if fileInfo.IsDir() {
		return http.StatusBadRequest, map[string]any{"exception_type": "FileNotFoundException"}
	}

	size := fileInfo.Size()
	return http.StatusOK, map[string]any{"size": size}
}

func (s *StorageServer) register() error {
	files, err := s.listFiles()
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
	defer resp.Body.Close()

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
		err := os.Remove(filepath.Join(s.directory, file))
		if err != nil {
			log.Printf("Failed to remove file %s: %v", file, err)
		}
	}

	return nil
}

func (s *StorageServer) listFiles() ([]string, error) {
	var files []string
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
