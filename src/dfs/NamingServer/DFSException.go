package NamingServer

type DFSException interface {
	Type() string
	Error() string
}

type FileNotFoundException struct {
	msg string
}

func (e *FileNotFoundException) Type() string {
	return "FileNotFoundException"
}

func (e *FileNotFoundException) Error() string {
	return e.msg
}
