package naming

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
	if pth == "/" {
		return []string{""}
	}
	return strings.Split(pth, "/")
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
	path          string
	parent        *Directory
	storageServer *StorageServerInfo
}

func (d *Directory) lockPath(names []string, readonly bool) *Directory {
	if len(names) == 0 {
		return nil
	}
	if names[0] != "" {
		return nil
	}
	var curr *Directory = nil
	final := names[len(names)-1]
	for _, name := range names[:len(names)-1] {
		if curr == nil {
			curr = d
			continue
		}
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
	if curr == nil {
		if readonly {
			d.lock.RLock()
		} else {
			d.lock.Lock()
		}
		return d
	}
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
	if dir == nil {
		return
	}
	parent := dir.parent
	if readonly {
		dir.lock.RUnlock()
	} else {
		dir.lock.Unlock()
	}
	dir = parent
	for dir != nil {
		parent = dir.parent
		dir.lock.RUnlock()
		dir = parent
	}
}

func (d *Directory) PathExists(pth string) (bool, bool, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return false, false, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		// pth is the root directory
		return true, false, nil
	}
	parent := d.lockPath(names[:len(names)-1], true)
	if parent == nil {
		return false, false, nil
	}
	defer d.unlockPath(parent, true)

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

func (d *Directory) MakeDirectory(pth string) (bool, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return false, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		return false, nil
	}

	// wlock parent directory
	parent := d.lockPath(names[:len(names)-1], false)
	if parent == nil {
		return false, &DFSException{FileNotFoundException, "the parent directory does not exist."}
	}
	defer d.unlockPath(parent, false)

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
	}
	parent.subDirectories = append(parent.subDirectories, newDir)
	return true, nil
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

func (d *Directory) CreateFile(pth string, storageServer *StorageServerInfo) (bool, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return false, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		// rejects root directory
		return false, nil
	}
	newFileName := names[len(names)-1]
	parent := d.lockPath(names[:len(names)-1], false)
	if parent == nil {
		return false, &DFSException{FileNotFoundException, "the parent directory does not exist."}
	}
	defer d.unlockPath(parent, false)

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
		return false, nil
	}

	newFile := &FileInfo{
		name:          newFileName,
		path:          path.Clean(pth),
		parent:        parent,
		storageServer: storageServer,
	}
	parent.subFiles = append(parent.subFiles, newFile)
	return true, nil
}

func (d *Directory) DeletePath(pth string) ([]*FileInfo, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return make([]*FileInfo, 0), &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	if len(names) == 1 {
		// cannot delete root directory
		return make([]*FileInfo, 0), nil
	}
	deleted := names[len(names)-1]
	parent := d.lockPath(names[:len(names)-1], false)
	if parent == nil {
		return make([]*FileInfo, 0), &DFSException{FileNotFoundException, fmt.Sprintf("path %s does not exist.", pth)}
	}
	defer d.unlockPath(parent, false)

	// find the directory or file to be deleted
	var deletedDir *Directory = nil
	var deletedFiles []*FileInfo = nil
	var index int
	for i, dir := range parent.subDirectories {
		if dir.name == deleted {
			deletedDir = dir
			index = i
			break
		}
	}
	for i, file := range parent.subFiles {
		if file.name == deleted {
			deletedFiles = append(deletedFiles, file)
			index = i
			break
		}
	}
	if deletedDir == nil && deletedFiles == nil {
		return make([]*FileInfo, 0), &DFSException{FileNotFoundException, fmt.Sprintf("path %s does not exist.", pth)}
	}

	// find all files to be deleted
	var findDeletedFilesDFS func(*Directory)
	findDeletedFilesDFS = func(currDir *Directory) {
		if currDir == nil {
			return
		}
		for _, file := range currDir.subFiles {
			deletedFiles = append(deletedFiles, file)
		}
		for _, dir := range currDir.subDirectories {
			findDeletedFilesDFS(dir)
		}
	}
	findDeletedFilesDFS(deletedDir)

	if deletedDir != nil {
		parent.subDirectories = append(parent.subDirectories[:index], parent.subDirectories[index+1:]...)
	} else {
		parent.subFiles = append(parent.subFiles[:index], parent.subFiles[index+1:]...)
	}
	return deletedFiles, nil
}

func (d *Directory) ListDir(pth string) ([]string, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return nil, &DFSException{IllegalArgumentException, fmt.Sprintf("path %s is illegal.", pth)}
	}
	dir := d.lockPath(names, true) // directory to be listed
	if dir == nil {
		return nil, &DFSException{FileNotFoundException, fmt.Sprintf("Cannot find directory %s.", pth)}
	}
	defer d.unlockPath(dir, true)
	itemNames := make([]string, 0)
	for _, file := range dir.subFiles {
		itemNames = append(itemNames, file.name)
	}
	for _, subdir := range dir.subDirectories {
		itemNames = append(itemNames, subdir.name)
	}
	return itemNames, nil
}

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
			name:          fileName,
			path:          path.Clean(pth),
			parent:        curr,
			storageServer: storageServer,
		}
		curr.subFiles = append(curr.subFiles, file)
		success = append(success, true)
	}
	return success
}
