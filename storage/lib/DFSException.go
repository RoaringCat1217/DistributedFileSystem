package storage

const IllegalArgumentException = "IllegalArgumentException"
const FileNotFoundException = "FileNotFoundException"
const IllegalStateException = "IllegalStateException"

type DFSException struct {
	Type string `json:"exception_type"`
	Msg  string `json:"exception_info"`
}
