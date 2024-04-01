package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileSystem represents the file system operations of the storage server.
type FileSystem struct {
	directory string
}

// ReadFile reads data from a file.
func (fs *FileSystem) ReadFile(path string, offset, length int64) ([]byte, *DFSException) {
	filePath := filepath.Join(fs.directory, path)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &DFSException{Type: FileNotFoundException, Msg: "File not found"}
		}
		return nil, &DFSException{Type: IOException, Msg: "Error opening file"}
	}
	defer file.Close()

	if length == -1 {
		fileInfo, err := file.Stat()
		if err != nil {
			return nil, &DFSException{Type: IOException, Msg: "Error getting file stats"}
		}
		length = fileInfo.Size()
	}

	buffer := make([]byte, length)
	_, err = file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return nil, &DFSException{Type: IOException, Msg: "Error reading file"}
	}

	return buffer, nil
}

func (fs *FileSystem) WriteFile(path string, data []byte, offset int64) *DFSException {
	filePath := filepath.Join(fs.directory, path)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error opening file for writing"}
	}
	defer file.Close()

	_, err = file.WriteAt(data, offset)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error writing to file"}
	}

	return nil
}

func (fs *FileSystem) GetFileSize(path string) (int64, *DFSException) {
	filePath := filepath.Join(fs.directory, path)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, &DFSException{Type: FileNotFoundException, Msg: "File not found"}
		}
		return 0, &DFSException{Type: IOException, Msg: "Error getting file stats"}
	}

	return fileInfo.Size(), nil
}

func (fs *FileSystem) CreateFile(path string) *DFSException {
	filePath := filepath.Join(fs.directory, path)
	_, err := os.Stat(filePath)
	if err == nil {
		// file already existed
		return &DFSException{Type: IllegalStateException, Msg: "File or directory already exists"}
	}
	if !os.IsNotExist(err) {
		return &DFSException{Type: IOException, Msg: "Error checking file existence"}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error creating file"}
	}
	file.Close()

	return nil
}

func (fs *FileSystem) DeleteFile(path string) *DFSException {
	filePath := filepath.Join(fs.directory, path)
	err := os.RemoveAll(filePath)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error deleting file or directory"}
	}

	return nil
}

func (fs *FileSystem) CopyFile(sourcePath, destinationPath string) *DFSException {
	sourceFilePath := filepath.Join(fs.directory, sourcePath)
	destinationFilePath := filepath.Join(fs.directory, destinationPath)

	data, err := os.ReadFile(sourceFilePath)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error reading source file"}
	}

	err = os.WriteFile(destinationFilePath, data, 0644)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error writing to destination file"}
	}

	return nil
}

// ListFiles lists all files in the directory.
func (fs *FileSystem) ListFiles() ([]string, error) {
	var files []string

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
	for _, path := range paths {
		fullPath := filepath.Join(fs.directory, path)
		if err := os.RemoveAll(fullPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", path, err)
		}
	}
	return nil
}
