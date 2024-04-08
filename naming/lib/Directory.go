package naming

import (
	"fmt"
	"math/rand"
	"path"
	"strings"
	"sync"
)

// pathToNames - decompose a path to a series of directory or file names
// The root directory has name ""
// returns nil if the path is invalid
func pathToNames(pth string) []string {
	pth = path.Clean(pth)
	if pth[0] != '/' {
		return nil
	}
	if pth == "/" {
		return []string{""}
	}
	return strings.Split(pth, "/")
}

// FSItem - Either a *Directory or a *FileInfo
// Designed to make accessing lock tables easier
type FSItem interface {
	GetParentDir() *Directory
	GetLock() *FIFORWMutex
}

// RLockedItem - One entry in the r-lock table
type RLockedItem struct {
	item  FSItem
	count int
}

// Directory - represents a directory in the DFS
// The root Directory is responsible for keeping track of all files and directories
// in the file system, and managing their locks.
type Directory struct {
	name           string
	parent         *Directory
	subDirectories []*Directory
	subFiles       []*FileInfo
	lock           *FIFORWMutex
	// list of r-locked files or directories
	rLockedItems    map[string]*RLockedItem
	rLockedItemsMtx sync.Mutex
	// list of w-locked files or directories
	wLockedItems    map[string]FSItem
	wLockedItemsMtx sync.Mutex
}

// GetParentDir - implements FSItem interface
func (d *Directory) GetParentDir() *Directory {
	return d.parent
}

// GetLock - implements FSItem interface
func (d *Directory) GetLock() *FIFORWMutex {
	return d.lock
}

// FileInfo - represents a file in one or multiple storage servers
type FileInfo struct {
	name   string
	path   string
	parent *Directory
	lock   *FIFORWMutex
	// fields used for replication
	// any access to these fields must acquire rCountMtx
	rCount         int
	rCountMtx      sync.Mutex
	storageServers []*StorageServerInfo
}

// GetParentDir - implements FSItem
func (f *FileInfo) GetParentDir() *Directory {
	return f.parent
}

// GetLock - implements FSItem
func (f *FileInfo) GetLock() *FIFORWMutex {
	return f.lock
}

// GetPath - return the absolute path of a directory
func (d *Directory) GetPath() string {
	names := make([]string, 0)
	curr := d
	for curr != nil {
		names = append(names, curr.name)
		curr = curr.parent
	}
	i := 0
	j := len(names) - 1
	for i < j {
		names[i], names[j] = names[j], names[i]
		i++
		j--
	}
	return strings.Join(names, "/")
}

// walkPath - a helper method, walks the directories specified in names
// if it succeeds, returns the last directory along the path
// if it fails, returns nil
func (d *Directory) walkPath(names []string) *Directory {
	if len(names) == 0 {
		return nil
	}
	if names[0] != "" {
		return nil
	}
	curr := d
	for _, name := range names[1:] {
		found := false
		for _, dir := range curr.subDirectories {
			if dir.name == name {
				found = true
				curr = dir
				break
			}
		}
		if !found {
			// cannot find a directory in the path
			return nil
		}
	}
	return curr
}

// lockPath - rlock every directory in a path specified in names
// if it succeeds, returns the last directory along the path
// if it fails, release every lock it has acquired and returns nil
func (d *Directory) lockPath(names []string) *Directory {
	if len(names) == 0 {
		return nil
	}
	if names[0] != "" {
		return nil
	}
	curr := d
	for _, name := range names[1:] {
		curr.lock.RLock()
		found := false
		for _, dir := range curr.subDirectories {
			if dir.name == name {
				found = true
				curr = dir
				break
			}
		}
		if !found {
			// cannot find a directory in the path
			d.unlockPath(curr)
			return nil
		}
	}
	curr.lock.RLock()
	return curr
}

// unlockPath - unlocks rlocks from directory dir all the way to root
func (d *Directory) unlockPath(dir *Directory) {
	if dir == nil {
		return
	}
	for dir != nil {
		parent := dir.parent
		dir.lock.RUnlock()
		dir = parent
	}
}

// PathExists - check whether a path corresponds to a file, a directory,
// or does not exist in the file system
// The first return value means whether the path is a directory
// The second return value means whether the path is a file
func (d *Directory) PathExists(pth string) (bool, bool, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return false, false, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		// pth is the root directory
		return true, false, nil
	}
	parent := d.walkPath(names[:len(names)-1])
	if parent == nil {
		return false, false, nil
	}

	itemName := names[len(names)-1]
	foundDir := false
	for _, dir := range parent.subDirectories {
		if dir.name == itemName {
			foundDir = true
			break
		}
	}
	foundFile := false
	for _, file := range parent.subFiles {
		if file.name == itemName {
			foundFile = true
			break
		}
	}
	return foundDir, foundFile, nil
}

// MakeDirectory - creates a new directory specified in pth
// Assumes the client holds the w-lock of its parent directory
func (d *Directory) MakeDirectory(pth string) (bool, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return false, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		return false, nil
	}

	// find parent directory
	parent := d.walkPath(names[:len(names)-1])
	if parent == nil {
		return false, &DFSException{FileNotFoundException, "the parent directory does not exist."}
	}

	newDirName := names[len(names)-1]
	// check if newDirName conflicts with existing files or directories
	existed := false
	for _, dir := range parent.subDirectories {
		if dir.name == newDirName {
			existed = true
			break
		}
	}
	for _, file := range parent.subFiles {
		if file.name == newDirName {
			existed = true
			break
		}
	}
	if existed {
		// already existed, just ignore it
		return false, nil
	}

	// create new directory
	newDir := &Directory{
		name:   newDirName,
		parent: parent,
		lock:   NewFIFORWMutex(),
	}
	parent.subDirectories = append(parent.subDirectories, newDir)
	return true, nil
}

// GetFileStorage - Get one of the storage servers that has a file
// Assumes the client holds the r-lock of the file
// If there are multiple possible storage servers, return a random one
func (d *Directory) GetFileStorage(pth string) (*StorageServerInfo, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	// rlock parent
	fileName := names[len(names)-1]
	parent := d.walkPath(names[:len(names)-1])
	if parent == nil {
		return nil, &DFSException{FileNotFoundException, fmt.Sprintf("cannot find file %s.", pth)}
	}

	for _, file := range parent.subFiles {
		if file.name == fileName {
			// choose a random storage server
			file.rCountMtx.Lock()
			storageServer := file.storageServers[rand.Intn(len(file.storageServers))]
			file.rCountMtx.Unlock()
			return storageServer, nil
		}
	}
	return nil, &DFSException{FileNotFoundException, fmt.Sprintf("cannot find file %s.", pth)}
}

// CreateFile - creates a new file in pth, and it is stored in storageServer
// Assumes the client has w-lock of its parent directory
func (d *Directory) CreateFile(pth string, storageServer *StorageServerInfo) (*FileInfo, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		// rejects root directory
		return nil, nil
	}
	newFileName := names[len(names)-1]
	parent := d.walkPath(names[:len(names)-1])
	if parent == nil {
		return nil, &DFSException{FileNotFoundException, "the parent directory does not exist."}
	}

	conflict := false
	for _, dir := range parent.subDirectories {
		if dir.name == newFileName {
			conflict = true
			break
		}
	}
	for _, file := range parent.subFiles {
		if file.name == newFileName {
			conflict = true
			break
		}
	}
	if conflict {
		return nil, nil
	}

	newFile := &FileInfo{
		name:   newFileName,
		path:   path.Clean(pth),
		parent: parent,
		lock:   NewFIFORWMutex(),
	}
	newFile.storageServers = append(newFile.storageServers, storageServer)
	parent.subFiles = append(parent.subFiles, newFile)
	return newFile, nil
}

// DeletePath - deletes a file or directory
// Assumes the client has w-lock of its parent directory
func (d *Directory) DeletePath(pth string) (FSItem, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		// cannot delete root directory
		return nil, nil
	}
	deleted := names[len(names)-1]
	parent := d.walkPath(names[:len(names)-1])
	if parent == nil {
		return nil, &DFSException{FileNotFoundException, fmt.Sprintf("path %s does not exist.", pth)}
	}

	// find the directory or file to be deleted
	var deletedDir *Directory = nil
	var deletedFile *FileInfo = nil
	var index int
	for i, dir := range parent.subDirectories {
		if dir.name == deleted {
			deletedDir = dir
			dir.lock.Destroy()
			index = i
			break
		}
	}
	for i, file := range parent.subFiles {
		if file.name == deleted {
			deletedFile = file
			file.lock.Destroy()
			index = i
			break
		}
	}
	if deletedDir == nil && deletedFile == nil {
		return nil, &DFSException{FileNotFoundException, fmt.Sprintf("path %s does not exist.", pth)}
	}

	if deletedDir != nil {
		parent.subDirectories = append(parent.subDirectories[:index], parent.subDirectories[index+1:]...)
		return deletedDir, nil
	}
	parent.subFiles = append(parent.subFiles[:index], parent.subFiles[index+1:]...)
	return deletedFile, nil
}

// ListDir - lists files in a directory
// Assumes the client has r-lock of the directory
func (d *Directory) ListDir(pth string) ([]string, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	dir := d.walkPath(names) // directory to be listed
	if dir == nil {
		return nil, &DFSException{FileNotFoundException, fmt.Sprintf("Cannot find directory %s.", pth)}
	}

	itemNames := make([]string, 0)
	for _, file := range dir.subFiles {
		itemNames = append(itemNames, file.name)
	}
	for _, subdir := range dir.subDirectories {
		itemNames = append(itemNames, subdir.name)
	}
	return itemNames, nil
}

// LockFileOrDirectory - locks a file or directory
// The locked file or directory is added to root directory's lock tables
func (d *Directory) LockFileOrDirectory(pth string, readonly bool) (FSItem, *DFSException) {
	pth = path.Clean(pth)
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	var fsItem FSItem = nil // the file or directory to be locked
	if len(names) == 1 {
		// request to lock the root directory
		fsItem = d
	} else {
		// try to find fsItem
		itemName := names[len(names)-1]
		parent := d.lockPath(names[:len(names)-1])
		if parent == nil {
			return nil, &DFSException{FileNotFoundException, "the file/directory cannot be found"}
		}
		for _, dir := range parent.subDirectories {
			if dir.name == itemName {
				fsItem = dir
				break
			}
		}
		for _, file := range parent.subFiles {
			if file.name == itemName {
				fsItem = file
				break
			}
		}
		if fsItem == nil {
			d.unlockPath(parent)
			return nil, &DFSException{FileNotFoundException, "the file/directory cannot be found"}
		}
	}
	if readonly {
		fsItem.GetLock().RLock()
		// add it to rLockedItems table
		d.rLockedItemsMtx.Lock()
		item, exists := d.rLockedItems[pth]
		if exists {
			item.count++
		} else {
			d.rLockedItems[pth] = &RLockedItem{fsItem, 1}
		}
		d.rLockedItemsMtx.Unlock()
	} else {
		fsItem.GetLock().Lock()
		// add it to wLockedItems table
		d.wLockedItemsMtx.Lock()
		d.wLockedItems[pth] = fsItem
		d.wLockedItemsMtx.Unlock()
	}
	return fsItem, nil
}

// UnlockFileOrDirectory - unlocks a file or directory
// It checks the root's lock tables to guarantee the file or directory
// is locked before and has the right lock type
func (d *Directory) UnlockFileOrDirectory(pth string, readonly bool) *DFSException {
	if len(pathToNames(pth)) == 0 {
		return &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	pth = path.Clean(pth)
	if readonly {
		d.rLockedItemsMtx.Lock()
		defer d.rLockedItemsMtx.Unlock()
		entry, exists := d.rLockedItems[pth]
		if !exists {
			return &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is not r-locked", pth)}
		}
		fsItem := entry.item
		entry.count--
		if entry.count == 0 {
			delete(d.rLockedItems, pth)
		}
		parent := fsItem.GetParentDir()
		fsItem.GetLock().RUnlock()
		d.unlockPath(parent)
	} else {
		d.wLockedItemsMtx.Lock()
		defer d.wLockedItemsMtx.Unlock()
		fsItem, exists := d.wLockedItems[pth]
		if !exists {
			return &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is not w-locked", pth)}
		}
		delete(d.wLockedItems, pth)
		parent := fsItem.GetParentDir()
		fsItem.GetLock().Unlock()
		d.unlockPath(parent)
	}
	return nil
}

// RegisterFiles - registers files from a newly registered storage server
// It may need to create many files and directories, so it w-locks the
// entire file system to prevent any deadlocks
func (d *Directory) RegisterFiles(pths []string, storageServer *StorageServerInfo) []bool {
	// lock the entire FS
	d.lock.Lock()
	defer d.lock.Unlock()

	success := make([]bool, 0)
	for i := range pths {
		pth := pths[i]
		names := pathToNames(pth)
		if len(names) == 0 {
			success = append(success, false)
			continue
		}
		if len(names) == 1 {
			// silently ignore "/" attempt
			success = append(success, true)
			continue
		}
		// ignore root directory
		names = names[1:]
		fileName := names[len(names)-1]
		curr := d
		failed := false
		for _, name := range names[:len(names)-1] {
			found := false
			for _, dir := range curr.subDirectories {
				if dir.name == name {
					found = true
					curr = dir
					break
				}
			}
			if !found {
				// try to create a new directory, if no conflicts
				for _, file := range curr.subFiles {
					if file.name == name {
						found = true
						break
					}
				}
				if found {
					// new directory's name conflicts with an existing file
					failed = true
					break
				}
				// create a new directory
				newDir := &Directory{
					name:   name,
					parent: curr,
					lock:   NewFIFORWMutex(),
				}
				curr.subDirectories = append(curr.subDirectories, newDir)
				curr = newDir
			}
		}
		if failed {
			success = append(success, false)
			continue
		}
		// check if fileName conflicts with existing files or directories
		for _, dir := range curr.subDirectories {
			if dir.name == fileName {
				failed = true
				break
			}
		}
		for _, file := range curr.subFiles {
			if file.name == fileName {
				failed = true
				break
			}
		}
		if failed {
			success = append(success, false)
			continue
		}
		// register the file
		file := &FileInfo{
			name:   fileName,
			path:   path.Clean(pth),
			parent: curr,
			lock:   NewFIFORWMutex(),
		}
		file.storageServers = append(file.storageServers, storageServer)
		curr.subFiles = append(curr.subFiles, file)
		success = append(success, true)
	}
	return success
}
