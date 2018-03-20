package services

import "gp_upgrade/helpers"

type AgentServer struct {
	GetDiskUsage  func() (map[string]float64, error)
	commandExecer helpers.CommandExecer
}

func NewAgentServer(execer helpers.CommandExecer) *AgentServer {
	return &AgentServer{
		GetDiskUsage:  diskUsage,
		commandExecer: execer,
	}
}
