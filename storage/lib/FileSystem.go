package storage

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileSystem represents the file system operations of the storage server.
type FileSystem struct {
	directory string
}

// isFile - Check if the path corresponds to an existing file
func (fs *FileSystem) checkFileExist(path string) (os.FileInfo, *DFSException) {
	if path == "" {
		return nil, &DFSException{IllegalArgumentException, "Path is invalid"}
	}
	filePath := filepath.Join(fs.directory, path)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &DFSException{FileNotFoundException, "Path not found"}
		}
		return nil, &DFSException{IOException, fmt.Sprintf("Error accessing path: %s", err.Error())}
	}
	if fileInfo.IsDir() {
		return nil, &DFSException{FileNotFoundException, "Path is not a file"}
	}
	return fileInfo, nil
}

// ReadFile reads data from a file.
func (fs *FileSystem) ReadFile(path string, offset, length int64) (string, *DFSException) {
	fileInfo, ex := fs.checkFileExist(path)
	if ex != nil {
		return "", ex
	}

	if offset < 0 || length < 0 || offset+length > fileInfo.Size() {
		return "", &DFSException{Type: IndexOutOfBoundsException, Msg: "Invalid offset or length"}
	}

	filePath := filepath.Join(fs.directory, path)
	file, err := os.Open(filePath)
	if err != nil {
		return "", &DFSException{Type: IOException, Msg: fmt.Sprintf("Error opening file: %s", err.Error())}
	}
	defer file.Close()

	buffer := make([]byte, length)
	_, err = file.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return "", &DFSException{Type: IOException, Msg: fmt.Sprintf("Error reading file: %s", err.Error())}
	}
	// encode with Base64
	encoded := base64.StdEncoding.EncodeToString(buffer)

	return encoded, nil
}

func (fs *FileSystem) WriteFile(path string, data string, offset int64) *DFSException {
	_, ex := fs.checkFileExist(path)
	if ex != nil {
		return ex
	}
	if offset < 0 {
		return &DFSException{Type: IndexOutOfBoundsException, Msg: "Invalid offset"}
	}

	filePath := filepath.Join(fs.directory, path)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error opening file for writing"}
	}
	defer file.Close()

	// decode base64 string
	decodedBytes, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return &DFSException{IOException, fmt.Sprintf("Error when decoding string: %s", err.Error())}
	}
	_, err = file.WriteAt(decodedBytes, offset)
	if err != nil {
		return &DFSException{Type: IOException, Msg: "Error writing to file"}
	}

	return nil
}

func (fs *FileSystem) GetFileSize(path string) (int64, *DFSException) {
	fileInfo, err := fs.checkFileExist(path)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (fs *FileSystem) CreateFile(path string) (bool, *DFSException) {
	if path == "" {
		return false, &DFSException{Type: IllegalArgumentException, Msg: "Invalid path"}
	}
	if path == "/" {
		return false, nil
	}

	filePath := filepath.Join(fs.directory, path)
	parentPath := filepath.Join(filePath, "../")
	// parent directory must exist
	dirInfo, err := os.Stat(parentPath)
	if err == nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(parentPath, 0777)
			if err != nil {
				return false, &DFSException{IOException, fmt.Sprintf("Error when creating directories: %s", err.Error())}
			}
		} else {
			return false, &DFSException{IOException, fmt.Sprintf("Error when opening parent directory: %s", err.Error())}
		}
	}
	if !dirInfo.IsDir() {
		return false, &DFSException{Type: IllegalArgumentException, Msg: "parent directory does not exist"}
	}
	// the file must not exist for now
	_, err = os.Stat(filePath)
	if err == nil {
		// a file or directory with the same path exists now
		return false, nil
	}
	// try to create the file
	file, err := os.Create(filePath)
	if err != nil {
		return false, &DFSException{IOException, err.Error()}
	}
	// created the file successfully
	file.Close()
	return true, nil
}

func (fs *FileSystem) DeleteFile(path string) (bool, *DFSException) {
	if path == "" {
		return false, &DFSException{IllegalArgumentException, "Path is invalid"}
	}
	filePath := filepath.Join(fs.directory, path)
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, &DFSException{FileNotFoundException, "Path not found"}
		}
		return false, &DFSException{IOException, fmt.Sprintf("Error accessing path: %s", err.Error())}
	}
	err = os.RemoveAll(filePath)
	if err != nil {
		return false, &DFSException{Type: IOException, Msg: fmt.Sprintf("Error deleting file or directory: %s", err.Error())}
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

func (fs *FileSystem) Prune() error {
	var pruneRecursive func(string) error
	pruneRecursive = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				subdir := filepath.Join(dir, entry.Name())
				if err := pruneRecursive(subdir); err != nil {
					return err
				}
			}
		}
		// ReadDir again because subdirectories can be pruned
		entries, err = os.ReadDir(dir)
		if err != nil {
			return err
		}
		if len(entries) == 0 && dir != fs.directory {
			err = os.Remove(dir)
			if err != nil {
				return err
			}
			return nil
		}
		return nil
	}
	return pruneRecursive(fs.directory)
}
