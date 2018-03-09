package services

type AgentServer struct {
	GetDiskUsage func() (map[string]float64, error)
}

func NewAgentServer() *AgentServer {
	return &AgentServer{
		GetDiskUsage: diskUsage,
	}
}
