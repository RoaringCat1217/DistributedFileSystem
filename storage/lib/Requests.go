package storage

type RegisterRequest struct {
	StorageIP   string   `json:"storage_ip"`
	ClientPort  int      `json:"client_port"`
	CommandPort int      `json:"command_port"`
	Files       []string `json:"files"`
}
type ReadRequest struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

type WriteRequest struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Data   string `json:"data"`
}

type DeleteRequest struct {
	Path string `json:"path"`
}

type CreateRequest struct {
	Path string `json:"path"`
}

type CopyRequest struct {
	Path       string `json:"path"`
	SourceAddr string `json:"server_ip"`
	SourcePort int    `json:"server_port"`
}

type SizeRequest struct {
	Path string `json:"path"`
}
