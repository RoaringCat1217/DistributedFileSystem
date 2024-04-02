package naming

import (
	"hash/fnv"
	"net/http"
	"path"
)

// handlers for client APIs
func (s *NamingServer) isValidPathHandler(body PathRequest) (int, any) {
	foundDir, foundFile, _ := s.root.PathExists(body.Path)
	return http.StatusOK, SuccessResponse{foundDir || foundFile}
}

func (s *NamingServer) getStorageHandler(body PathRequest) (int, any) {
	storageServer, err := s.root.GetFileStorage(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, StorageInfoResponse{"127.0.0.1", storageServer.clientPort}
}

func (s *NamingServer) createDirectoryHandler(body PathRequest) (int, any) {
	success, err := s.root.MakeDirectory(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, SuccessResponse{success}
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

	success, err := s.root.CreateFile(body.Path, storageServer)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, SuccessResponse{success}
}

func (s *NamingServer) listDirHandler(body PathRequest) (int, any) {
	files, err := s.root.ListDir(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, ListFilesResponse{files}
}

func (s *NamingServer) isDirectoryHandler(body PathRequest) (int, any) {
	foundDir, foundFile, err := s.root.PathExists(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	if !foundDir && !foundFile {
		return http.StatusNotFound, &DFSException{FileNotFoundException, "the file/directory or parent directory does not exist."}
	}
	return http.StatusOK, SuccessResponse{foundDir}
}

func (s *NamingServer) lockHandler(body LockRequest) (int, any) {
	err := s.root.LockFileOrDirectory(body.Path, !body.Exclusive)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, nil
}

func (s *NamingServer) unlockHandler(body LockRequest) (int, any) {
	err := s.root.UnlockFileOrDirectory(body.Path, !body.Exclusive)
	if err != nil {
		return http.StatusNotFound, err
	}
	return http.StatusOK, nil
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
