package services

import (
	"context"
	"fmt"
	"os/exec"

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

	defer func() {
		if r := recover(); r != nil {
			gplog.Error("Recovered from netstat command: %s", r)
		}
	}()

	gplog.Info("Running command %s using command execer %+v", cmd, s.commandExecer)
	//findConflictPortCmd := s.commandExecer("bash", "-c", cmd)
	findConflictPortCmd := exec.Command("bash", "-c", cmd)

	gplog.Info("Finished running command %s", cmd)

	gplog.Info("findConflictPortCmd is %v, output is %v", findConflictPortCmd, findConflictPortCmd.Output)
	output, err := findConflictPortCmd.Output()
	gplog.Info("Finished checking port range, output was %s", string(output))
	if err != nil {
		gplog.Info("Error was %s", err.Error())
		return &pb.CheckFreePortsReply{}, err
	}

	if string(output) == "" {
		gplog.Info("Port range is free")
		return &pb.CheckFreePortsReply{Result: true}, nil
	}
	gplog.Info("Port range is not free")
	return &pb.CheckFreePortsReply{Result: false}, nil
}
