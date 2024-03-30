package StorageServer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Directory struct {
	name           string
	parent         *Directory
	subDirectories []*Directory
	subFiles       []*FileInfo
	lock           sync.RWMutex
}

type FileInfo struct {
	name string
	hash string
}

func pathToNames(pth string) []string {
	pth = filepath.Clean(pth)
	if pth[0] != '/' {
		return nil
	}
	return strings.Split(pth, "/")[1:]
}

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
	found := false
	for _, file := range curr.subFiles {
		if file.name == name {
			found = true
			break
		}
	}
	d.rUnlockDirs(curr)
	return found
}

func (d *Directory) CreateFile(pth string, hash string) *DFSException {
	names := pathToNames(pth)
	if len(names) == 0 {
		return &DFSException{Type: IllegalArgumentException, Msg: fmt.Sprintf("path %s is illegal.", pth)}
	}
	var grandpa *Directory = nil
	var parent *Directory = nil
	if len(names) == 1 {
		parent = d
	} else {
		grandpa = d.rLockDirs(names[:len(names)-2])
		if grandpa == nil {
			return &DFSException{Type: FileNotFoundException, Msg: "the parent directory does not exist."}
		}
		for _, dir := range grandpa.subDirectories {
			if dir.name == names[len(names)-2] {
				parent = dir
				break
			}
		}
		if parent == nil {
			d.rUnlockDirs(grandpa)
			return &DFSException{Type: FileNotFoundException, Msg: "the parent directory does not exist."}
		}
	}
	parent.lock.Lock()
	newFileName := names[len(names)-1]
	for _, file := range parent.subFiles {
		if file.name == newFileName {
			parent.lock.Unlock()
			if grandpa != nil {
				d.rUnlockDirs(grandpa)
			}
			return &DFSException{Type: FileNotFoundException, Msg: "file already exists."}
		}
	}
	newFile := &FileInfo{
		name: newFileName,
		hash: hash,
	}
	parent.subFiles = append(parent.subFiles, newFile)
	parent.lock.Unlock()
	if grandpa != nil {
		d.rUnlockDirs(grandpa)
	}
	return nil
}

func (d *Directory) DeleteFile(pth string) *DFSException {
	names := pathToNames(pth)
	if len(names) == 0 {
		return &DFSException{Type: IllegalArgumentException, Msg: fmt.Sprintf("path %s is illegal.", pth)}
	}
	fileName := names[len(names)-1]
	parent := d.rLockDirs(names[:len(names)-1])
	if parent == nil {
		return &DFSException{Type: FileNotFoundException, Msg: fmt.Sprintf("cannot find file %s.", pth)}
	}
	defer d.rUnlockDirs(parent)
	for i, file := range parent.subFiles {
		if file.name == fileName {
			parent.subFiles = append(parent.subFiles[:i], parent.subFiles[i+1:]...)
			return nil
		}
	}
	return &DFSException{Type: FileNotFoundException, Msg: fmt.Sprintf("cannot find file %s.", pth)}
}

func (d *Directory) GetFileHash(pth string) (string, *DFSException) {
	names := pathToNames(pth)
	if len(names) == 0 {
		return "", &DFSException{Type: IllegalArgumentException, Msg: fmt.Sprintf("path %s is illegal.", pth)}
	}
	fileName := names[len(names)-1]
	parent := d.rLockDirs(names[:len(names)-1])
	if parent == nil {
		return "", &DFSException{Type: FileNotFoundException, Msg: fmt.Sprintf("cannot find file %s.", pth)}
	}
	defer d.rUnlockDirs(parent)
	for _, file := range parent.subFiles {
		if file.name == fileName {
			return file.hash, nil
		}
	}
	return "", &DFSException{Type: FileNotFoundException, Msg: fmt.Sprintf("cannot find file %s.", pth)}
}

func (d *Directory) SaveMetadata(dir string) error {
	d.lock.RLock()
	defer d.lock.RUnlock()

	metadataPath := filepath.Join(dir, "metadata.json")
	metadataFile, err := os.Create(metadataPath)
	if err != nil {
		return err
	}
	defer func(metadataFile *os.File) {
		err := metadataFile.Close()
		if err != nil {

		}
	}(metadataFile)

	encoder := json.NewEncoder(metadataFile)
	return encoder.Encode(d)
}

func (d *Directory) LoadMetadata(dir string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	metadataPath := filepath.Join(dir, "metadata.json")
	metadataFile, err := os.Open(metadataPath)
	if err != nil {
		return err
	}
	defer func(metadataFile *os.File) {
		err := metadataFile.Close()
		if err != nil {

		}
	}(metadataFile)

	decoder := json.NewDecoder(metadataFile)
	return decoder.Decode(d)
}
