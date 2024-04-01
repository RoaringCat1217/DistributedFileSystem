package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileSystem represents the file system operations of the storage server.
type FileSystem struct {
	directory string
	mutex     sync.RWMutex
}

// NewFileSystem creates a new instance of FileSystem.
func NewFileSystem(directory string) *FileSystem {
	return &FileSystem{
		directory: directory,
	}
}

// ReadFile reads data from a file.
func (fs *FileSystem) ReadFile(path string, offset, length int64) ([]byte, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()
	filePath := filepath.Join(fs.directory, path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return nil, errors.New("file not found")
	}
	if fileInfo.IsDir() {
		return nil, errors.New("cannot read a directory")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	_, err = file.Read(data)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return data, nil
}

// WriteFile writes data to a file.
func (fs *FileSystem) WriteFile(path string, data []byte, offset int64) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	filePath := filepath.Join(fs.directory, path)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if _, err = file.Write(data); err != nil {
		return err
	}

	return nil
}

// GetFileSize gets the size of a file.
func (fs *FileSystem) GetFileSize(path string) (int64, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()
	filePath := filepath.Join(fs.directory, path)
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return 0, errors.New("file not found")
	}
	if fileInfo.IsDir() {
		return 0, errors.New("cannot get size of a directory")
	}

	return fileInfo.Size(), nil
}

// CreateFile creates a new file.
func (fs *FileSystem) CreateFile(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	filePath := filepath.Join(fs.directory, path)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	file.Close()

	return nil
}

// DeleteFile deletes a file or directory.
func (fs *FileSystem) DeleteFile(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	filePath := filepath.Join(fs.directory, path)
	return os.RemoveAll(filePath)
}

// CopyFile copies a file from another storage server.
func (fs *FileSystem) CopyFile(sourcePath, destinationPath string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	destPath := filepath.Join(fs.directory, destinationPath)
	if err := os.WriteFile(destPath, data, os.ModePerm); err != nil {
		return err
	}

	return nil
}

// ListFiles lists all files in the directory.
func (fs *FileSystem) ListFiles() ([]string, error) {
	var files []string
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	err := filepath.Walk(fs.directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(fs.directory, path)
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

// DeleteFiles deletes a list of files or directories.
func (fs *FileSystem) DeleteFiles(paths []string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	for _, path := range paths {
		fullPath := filepath.Join(fs.directory, path)
		if err := os.RemoveAll(fullPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", path, err)
		}
	}
	return nil
}
