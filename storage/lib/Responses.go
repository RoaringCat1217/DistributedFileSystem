package storage

type RegisterResponse struct {
	Files []string `json:"files" binding:"required"`
}

type SuccessResponse struct {
	Success bool `json:"success" binding:"required"`
}
