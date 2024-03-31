package storage

const IllegalArgumentException = "IllegalArgumentException"
const FileNotFoundException = "FileNotFoundException"
const IllegalStateException = "IllegalStateException"
const IOException = "IOException"

type DFSException struct {
	Type string `json:"exception_type"`
	Msg  string `json:"exception_info"`
}
