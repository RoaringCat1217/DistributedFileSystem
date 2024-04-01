package storage

type RegisterResponse struct {
	Files []string `json:"files" binding:"required"`
}

type ReadResponse struct {
	Data string `json:"data" binding:"required"`
}

type SizeResponse struct {
	Size int64 `json:"size" binding:"required"`
}

type SuccessResponse struct {
	Success bool `json:"success" binding:"required"`
}
