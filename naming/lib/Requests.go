package naming

type PathRequest struct {
	Path string `json:"path" binding:"required"`
}

type GetStorageRequest struct {
	Path string `json:"path" binding:"required"`
}

type LockRequest struct {
	Path      string `json:"path" binding:"required"`
	Exclusive bool   `json:"exclusive" binding:"required"`
}

type RegisterRequest struct {
	StorageIP   string   `json:"storage_ip" binding:"required"`
	ClientPort  int      `json:"client_port" binding:"required"`
	CommandPort int      `json:"command_port" binding:"required"`
	Files       []string `json:"files" binding:"required"`
}
