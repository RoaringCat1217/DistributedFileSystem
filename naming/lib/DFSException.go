package naming

const (
	IllegalArgumentException = "IllegalArgumentException"
	FileNotFoundException    = "FileNotFoundException"
	IllegalStateException    = "IllegalStateException"
)

// DFSException - exceptions sent from naming server to a client
type DFSException struct {
	Type string `json:"exception_type"`
	Msg  string `json:"exception_info"`
}
