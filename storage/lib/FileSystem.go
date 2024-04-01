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
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &DFSException{Type: FileNotFoundException, Msg: "File not found"}
		}
		return nil, &DFSException{Type: IOException, Msg: "Error accessing file"}
	}
	if fileInfo.IsDir() {
		return nil, &DFSException{Type: FileNotFoundException, Msg: "Path is a directory"}
	}

	if offset < 0 || length < 0 || offset+length > fileInfo.Size() {
		return nil, &DFSException{Type: IndexOutOfBoundsException, Msg: "Invalid offset or length"}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, &DFSException{Type: IOException, Msg: "Error opening file"}
	}
	defer file.Close()

	buffer := make([]byte, length)
	_, err = file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return nil, &DFSException{Type: IOException, Msg: "Error reading file"}
	}

	return buffer, nil
}

func (fs *FileSystem) WriteFile(path string, data []byte, offset int64) *DFSException {
	filePath := filepath.Join(fs.directory, path)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &DFSException{Type: FileNotFoundException, Msg: "File not found"}
		}
		return &DFSException{Type: IOException, Msg: "Error accessing file"}
	}
	if fileInfo.IsDir() {
		return &DFSException{Type: FileNotFoundException, Msg: "Path is a directory"}
	}

	if offset < 0 {
		return &DFSException{Type: IndexOutOfBoundsException, Msg: "Invalid offset"}
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
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
		return 0, &DFSException{Type: IllegalArgumentException, Msg: "Invalid path"}
	}
	if fileInfo.IsDir() {
		return 0, &DFSException{Type: FileNotFoundException, Msg: "Path is a directory"}
	}

	return fileInfo.Size(), nil
}

func (fs *FileSystem) CreateFile(path string) (bool, *DFSException) {
	if path == "/" {
		return false, &DFSException{Type: IllegalArgumentException, Msg: "Invalid path"}
	}

	filePath := filepath.Join(fs.directory, path)
	_, err := os.Stat(filePath)
	if err == nil {
		return false, &DFSException{Type: IllegalStateException, Msg: "File or directory already exists"}
	}
	if !os.IsNotExist(err) {
		return false, &DFSException{Type: IOException, Msg: "Error checking file existence"}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return false, &DFSException{Type: IOException, Msg: "Error creating file"}
	}
	file.Close()

	return true, nil
}

func (fs *FileSystem) DeleteFile(path string) (bool, *DFSException) {
	if path == "/" {
		return false, &DFSException{Type: IllegalArgumentException, Msg: "Cannot delete root directory"}
	}

	filePath := filepath.Join(fs.directory, path)
	err := os.RemoveAll(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, &DFSException{Type: FileNotFoundException, Msg: "File not found"}
		}
		return false, &DFSException{Type: IOException, Msg: "Error deleting file or directory"}
	}

	return true, nil
}

func (fs *FileSystem) CopyFile(sourcePath, destinationPath string) *DFSException {
	sourceFilePath := filepath.Join(fs.directory, sourcePath)
	destinationFilePath := filepath.Join(fs.directory, destinationPath)

	sourceFileInfo, err := os.Stat(sourceFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &DFSException{Type: FileNotFoundException, Msg: "Source file not found"}
		}
		return &DFSException{Type: IOException, Msg: "Error accessing source file"}
	}
	if sourceFileInfo.IsDir() {
		return &DFSException{Type: FileNotFoundException, Msg: "Source path is a directory"}
	}

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