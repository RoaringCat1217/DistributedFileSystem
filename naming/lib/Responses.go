package naming

type SuccessResponse struct {
	Success bool `json:"success" binding:"required"`
}

type ListFilesResponse struct {
	Files []string `json:"files" binding:"required"`
}

type StorageInfoResponse struct {
	ServiceIP   string `json:"server_ip" binding:"required"`
	ServicePort int    `json:"server_port" binding:"required"`
}
