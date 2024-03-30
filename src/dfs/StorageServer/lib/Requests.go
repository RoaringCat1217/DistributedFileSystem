package StorageServer

type ReadRequest struct {
	Path   string  `json:"path" binding:"required"`
	Offset float64 `json:"offset" binding:"required"`
	Length float64 `json:"length" binding:"required"`
}

type WriteRequest struct {
	Path   string  `json:"path" binding:"required"`
	Offset float64 `json:"offset" binding:"required"`
	Data   string  `json:"data" binding:"required"`
}

type DeleteRequest struct {
	Path string `json:"path" binding:"required"`
}

type CreateRequest struct {
	Path string `json:"path" binding:"required"`
}

type CopyRequest struct {
	Path       string  `json:"path" binding:"required"`
	SourceAddr string  `json:"source_addr" binding:"required"`
	SourcePort float64 `json:"source_port" binding:"required"`
}

type SizeRequest struct {
	Path string `json:"path" binding:"required"`
}
