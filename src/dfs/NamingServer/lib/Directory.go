package NamingServer

import (
	"fmt"
	"path"
	"strings"
	"sync"
)

func pathToNames(pth string) []string {
	pth = path.Clean(pth)
	if pth[0] != '/' {
		return nil
	}
	return strings.Split(pth, "/")[1:]
}

type Directory struct {
	name           string
	parent         *Directory
	subDirectories []*Directory
	subFiles       []*FileInfo
	lock           sync.RWMutex
}

type FileInfo struct {
	name          string
	parent        *Directory
	storageServer *StorageServerInfo
}

// rLockDirectories - RLock root and every directory specified in names
// returns the final locked directory if successful
// release everything it locks and returns nil if failed
func (d *Directory) rLockDirs(names []string) *Directory {
	curr := d
	for _, name := range names {
		curr.lock.RLock()
		foundNextDir := false
		for _, dir := range curr.subDirectories {
			if dir.name == name {
				curr = dir
				foundNextDir = true
				break
			}
		}
		if !foundNextDir {
			d.rUnlockDirs(curr)
			return nil
		}
	}
	curr.lock.RLock()
	return curr
}

// rUnlockDirectories - RUnlock the curr directory and all of its ancestors
func (d *Directory) rUnlockDirs(curr *Directory) {
	for curr != nil {
		parent := curr.parent
		curr.lock.RUnlock()
		curr = parent
	}
}

func (d *Directory) PathExists(pth string) bool {
	names := pathToNames(pth)
	if len(names) == 0 {
		return false
	}
	curr := d.rLockDirs(names[:len(names)-1])
	if curr == nil {
		return false
	}
	name := names[len(names)-1]
	// either a directory or a file is ok
	found := false
	for _, dir := range curr.subDirectories {
		if dir.name == name {
			found = true
			break
		}
	}
	for _, file := range curr.subFiles {
		if file.name == name {
			found = true
			break
		}
	}
	d.rUnlockDirs(curr)
	return found
}

func (d *Directory) MakeDirectory(pth string) *DFSException {
	names := pathToNames(pth)
	if len(names) == 0 {
		return &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	var grandpa *Directory = nil // parent directory's parent
	var parent *Directory = nil  // parent directory of the new directory
	if len(names) == 1 {
		parent = d
	} else {
		grandpa = d.rLockDirs(names[:len(names)-2])
		if grandpa == nil {
			return &DFSException{FileNotFoundException, "the parent directory does not exist."}
		}
		for _, dir := range grandpa.subDirectories {
			if dir.name == names[len(names)-2] {
				parent = dir
				break
			}
		}
		if parent == nil {
			d.rUnlockDirs(grandpa)
			return &DFSException{FileNotFoundException, "the parent directory does not exist."}
		}
	}
	// Lock the parent directory
	parent.lock.Lock()
	newDirName := names[len(names)-1]
	failed := false
	for _, dir := range parent.subDirectories {
		if dir.name == newDirName {
			failed = true
			break
		}
	}
	for _, file := range parent.subFiles {
		if file.name == newDirName {
			failed = true
			break
		}
	}
	if failed {
		// already existed
		parent.lock.Unlock()
		if grandpa != nil {
			d.rUnlockDirs(grandpa)
		}
		return &DFSException{FileNotFoundException, "directory's name conflicts with existing directories or files."}
	}
	// create new directory
	newDir := &Directory{
		name:   newDirName,
		parent: parent,
	}
	parent.subDirectories = append(parent.subDirectories, newDir)
	parent.lock.Unlock()
	if grandpa != nil {
		d.rUnlockDirs(grandpa)
	}
	return nil
}

func (d *Directory) GetFileStorage(pth string) (*StorageServerInfo, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	fileName := names[len(names)-1]
	parent := d.rLockDirs(names[:len(names)-1])
	if parent == nil {
		return nil, &DFSException{FileNotFoundException, fmt.Sprintf("cannot find file %s.", pth)}
	}
	defer d.rUnlockDirs(parent)
	for _, file := range parent.subFiles {
		if file.name == fileName {
			return file.storageServer, nil
		}
	}
	return nil, &DFSException{FileNotFoundException, fmt.Sprintf("cannot find file %s.", pth)}
}

func (d *Directory) RegisterFiles(pths []string, files []*FileInfo) []bool {
	// lock the entire FS
	d.lock.Lock()
	defer d.lock.Unlock()
}
