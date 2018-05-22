package services

import (
	"context"
	"os"

	pb "github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func (s *AgentServer) CreateSegmentDataDirectories(ctx context.Context, in *pb.CreateSegmentDataDirRequest) (*pb.CreateSegmentDataDirReply, error) {
	gplog.Info("got a request to create segment data directories from the hub")

	datadirs := in.Datadirs
	for _, segDataDir := range datadirs {
		err := os.Mkdir(segDataDir, 0755)
		if err != nil {
			return &pb.CreateSegmentDataDirReply{}, err
		}
	}
	return &pb.CreateSegmentDataDirReply{}, nil
}
