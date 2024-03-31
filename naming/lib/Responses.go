package naming

type SuccessResponse struct {
	Success bool `json:"success" binding:"required"`
}

type StorageInfoResponse struct {
	ServiceIP   string `json:"service_ip" binding:"required"`
	ServicePort int    `json:"server_port" binding:"required"`
}
