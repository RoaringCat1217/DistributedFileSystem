package naming

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// commands for storage servers
func (s *NamingServer) storageCreateCommand(file *FileInfo) {
	url := fmt.Sprintf("http://localhost:%d/storage_create", file.storageServer.commandPort)
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
		fmt.Printf("storage_create failed for file %s (storage server %v)\n", file.path, file.storageServer)
		return
	}
}
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
