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

func (d *Directory) lockPath(names []string, readonly bool) *Directory {
	if len(names) == 0 {
		if readonly {
			d.lock.RLock()
		} else {
			d.lock.Lock()
		}
		return d
	}
	curr := d
	final := names[len(names)-1]
	for _, name := range names[:len(names)-1] {
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
			d.unlockPath(curr, true)
			return nil
		}
	}
	// curr is the parent of final
	curr.lock.RLock()
	for _, dir := range curr.subDirectories {
		if dir.name == final {
			if readonly {
				dir.lock.RLock()
			} else {
				dir.lock.Lock()
			}
			return dir
		}
	}
	// cannot find final
	d.unlockPath(curr, true)
	return nil
}

func (d *Directory) unlockPath(dir *Directory, readonly bool) {
	if readonly {
		dir.lock.RUnlock()
	} else {
		dir.lock.Unlock()
	}
	dir = dir.parent
	for dir != nil {
		parent := dir.parent
		dir.lock.RUnlock()
		dir = parent
	}
}

func (d *Directory) PathExists(pth string) bool {
	names := pathToNames(pth)
	if len(names) == 0 {
		return false
	}
	parent := d.lockPath(names[:len(names)-1], true)
	if parent == nil {
		return false
	}
	defer d.unlockPath(parent, true)

	itemName := names[len(names)-1]
	// either a directory or a file is ok
	found := false
	for _, dir := range parent.subDirectories {
		if dir.name == itemName {
			found = true
			break
		}
	}
	for _, file := range parent.subFiles {
		if file.name == itemName {
			found = true
			break
		}
	}
	return found
}

func (d *Directory) MakeDirectory(pth string) *DFSException {
	names := pathToNames(pth)
	if len(names) == 0 {
		return &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}

	// wlock parent directory
	parent := d.lockPath(names[:len(names)-1], false)
	if parent == nil {
		return &DFSException{FileNotFoundException, "the parent directory does not exist."}
	}
	defer d.unlockPath(parent, false)

	newDirName := names[len(names)-1]
	// check if newDirName conflicts with existing files or directories
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
		return &DFSException{FileNotFoundException, "directory's name conflicts with existing directories or files."}
	}

	// create new directory
	newDir := &Directory{
		name:   newDirName,
		parent: parent,
	}
	parent.subDirectories = append(parent.subDirectories, newDir)
	return nil
}

func (d *Directory) GetFileStorage(pth string) (*StorageServerInfo, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	// rlock parent
	fileName := names[len(names)-1]
	parent := d.lockPath(names[:len(names)-1], true)
	if parent == nil {
		return nil, &DFSException{FileNotFoundException, fmt.Sprintf("cannot find file %s.", pth)}
	}
	defer d.unlockPath(parent, true)

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

	success := make([]bool, 0)
	for i := range pths {
		pth := pths[i]
		file := files[i]
		names := pathToNames(pth)
		if len(names) == 0 {
			success = append(success, false)
			continue
		}
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
				}
				curr.subDirectories = append(curr.subDirectories, curr)
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
		curr.subFiles = append(curr.subFiles, file)
		file.parent = curr
		success = append(success, true)
	}
	return success
}
