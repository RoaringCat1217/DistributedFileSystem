package storage

type RegisterRequest struct {
	StorageIP   string   `json:"storage_ip"`
	ClientPort  int      `json:"client_port"`
	CommandPort int      `json:"command_port"`
	Files       []string `json:"files"`
}
type ReadRequest struct {
	Path   string  `json:"path"`
	Offset float64 `json:"offset" binding:"required"`
	Length float64 `json:"length" binding:"required"`
}

type WriteRequest struct {
	Path   string  `json:"path"`
	Offset float64 `json:"offset" binding:"required"`
	Data   string  `json:"data" binding:"required"`
}

type DeleteRequest struct {
	Path string `json:"path"`
}

type CreateRequest struct {
	Path string `json:"path"`
}

type CopyRequest struct {
	Path       string  `json:"path"`
	SourceAddr string  `json:"source_addr" binding:"required"`
	SourcePort float64 `json:"source_port" binding:"required"`
}

type SizeRequest struct {
	Path string `json:"path"`
}
