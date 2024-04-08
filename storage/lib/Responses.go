package storage

type RegisterResponse struct {
	Files []string `json:"files"`
}

type ReadResponse struct {
	Data string `json:"data"`
}

type SizeResponse struct {
	Size int64 `json:"size"`
}

type SuccessResponse struct {
	Success bool `json:"success"`
}
