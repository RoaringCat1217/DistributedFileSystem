package naming

import (
	"math/rand"
	"net/http"
	"sync"
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
	deletedItem, err := s.root.DeletePath(body.Path)
	if err != nil {
		return http.StatusNotFound, err
	}
	if deletedItem == nil {
		return http.StatusOK, SuccessResponse{false}
	}

	var wg sync.WaitGroup
	if deletedFile, ok := deletedItem.(*FileInfo); ok {
		// notify the storage servers asynchronously
		for _, storageServer := range deletedFile.storageServers {
			storageServer := storageServer
			wg.Add(1)
			go s.storageDeleteCommand(deletedFile.path, storageServer, &wg)
		}
	} else {
		deletedDir := deletedItem.(*Directory)
		s.lock.RLock()
		defer s.lock.RUnlock()
		for _, storageServer := range s.storageServers {
			storageServer := storageServer
			wg.Add(1)
			go s.storageDeleteCommand(deletedDir.GetPath(), storageServer, &wg)
		}
	}
	wg.Wait()
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
	// allocate a random storage server
	idx := rand.Intn(len(s.storageServers))
	storageServer := s.storageServers[idx]
	s.lock.RUnlock()

	file, err := s.root.CreateFile(body.Path, storageServer)
	if err != nil {
		return http.StatusNotFound, err
	}
	success := file != nil
	if success {
		// notify the storage server
		s.storageCreateCommand(file)
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
	fsItem, err := s.root.LockFileOrDirectory(body.Path, !body.Exclusive)
	if err != nil {
		return http.StatusNotFound, err
	}
	if file, ok := fsItem.(*FileInfo); ok {
		// handles replication for the file
		file.rCountMtx.Lock()
		defer file.rCountMtx.Unlock()
		if body.Exclusive {
			// delete all except one replicas
			file.rCount = 0
			var wg sync.WaitGroup
			for _, storageServer := range file.storageServers[1:] {
				storageServer := storageServer
				wg.Add(1)
				go s.storageDeleteCommand(file.path, storageServer, &wg)
			}
			wg.Wait()
		} else {
			file.rCount++
			if file.rCount >= 20 {
				file.rCount -= 20
				// have one more replica, if possible
				s.lock.RLock()
				candidates := make([]*StorageServerInfo, 0)
				for _, storageServer := range s.storageServers {
					exists := false
					for _, currServer := range file.storageServers {
						if storageServer == currServer {
							exists = true
							break
						}
					}
					if !exists {
						candidates = append(candidates, storageServer)
					}
				}
				s.lock.RUnlock()
				if len(candidates) > 0 {
					// choose a random storage server to replicate
					dst := candidates[rand.Intn(len(candidates))]
					// choose a random storage server as source
					src := file.storageServers[rand.Intn(len(file.storageServers))]
					success := s.storageCopyCommand(file, dst, src)
					if success {
						file.storageServers = append(file.storageServers, dst)
					}
				}
			}
		}
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
