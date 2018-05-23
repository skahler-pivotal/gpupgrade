package services

import (
	"context"
	"fmt"

	pb "github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func (s *AgentServer) CheckFreePorts(ctx context.Context, in *pb.CheckFreePortsRequest) (*pb.CheckFreePortsReply, error) {
	gplog.Info("got a request to check free ports from the hub")

	startPort := in.PossiblePortBase
	endPort := in.PossiblePortBase + in.NumPrimaries

	gplog.Info("Checking port range from %d to %d", startPort, endPort)
	// Find if there are any conflicting ports within the range
	cmd := fmt.Sprintf(`netstat -n | grep -o '\.[0-9]*$' | grep -o '[0-9]*$' | uniq | sort | awk '{if ($1 >= %d && $1 <= %d) print $1}'`, startPort, endPort)

	findConflictPortCmd := s.commandExecer("bash", "-c", cmd)

	output, err := findConflictPortCmd.Output()
	if err != nil {
		return &pb.CheckFreePortsReply{}, err
	}

	if string(output) == "" {
		return &pb.CheckFreePortsReply{Result: true}, nil
	}
	return &pb.CheckFreePortsReply{Result: false}, nil
}
