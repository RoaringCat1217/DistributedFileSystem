package naming

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// commands sent from the naming server to storage servers

// storageCreateCommand - create a new file on a storage server
// Storage server is specified in file.storageServers
func (s *NamingServer) storageCreateCommand(file *FileInfo) {
	url := fmt.Sprintf("http://localhost:%d/storage_create", file.storageServers[0].commandPort)
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
		fmt.Printf("storage_create failed for file %s (storage server %v)\n", file.path, file.storageServers[0])
		return
	}
}

// storageDeleteCommand - send delete command to storageServer
// This method is called asynchronously in a goroutine and use wg to synchronize with caller
func (s *NamingServer) storageDeleteCommand(path string, storageServer *StorageServerInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	url := fmt.Sprintf("http://localhost:%d/storage_delete", storageServer.commandPort)
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"path":"%s"}`, path)))
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
		fmt.Printf("storage_delete failed for file %s (storage server %v)\n", path, storageServer)
		return
	}
}

// storageCopyCommand - send copy command to dst, asking it to copy from src
func (s *NamingServer) storageCopyCommand(file *FileInfo, dst *StorageServerInfo, src *StorageServerInfo) bool {
	url := fmt.Sprintf("http://localhost:%d/storage_copy", dst.commandPort)
	body := bytes.NewReader([]byte(fmt.Sprintf(`{"path":"%s", "server_ip": "127.0.0.1", "server_port": %d}`, file.path, src.clientPort)))
	resp, err := http.Post(url, "application/json", body)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	var success SuccessResponse
	err = json.Unmarshal(data, &success)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	if !success.Success {
		fmt.Printf("storeage_copy failed for file %s (dst %v, src %v)\n", file.path, dst, src)
		return false
	}
	return true
}
