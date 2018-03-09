package services

import (
	"context"

	"errors"
	"fmt"
	pb "gp_upgrade/idl"
)

func (s *AgentServer) CheckConversionStatus(ctx context.Context, in *pb.CheckConversionStatusRequest) (*pb.CheckConversionStatusReply, error) {
	if len(in.GetSegments()) == 0 {
		return nil, errors.New("no segment information was passed to the agent")
	}
	format := "PENDING - DBID %d - CONTENT ID %d - %s - %s"

	var replies []string
	var master string
	for _, segment := range in.GetSegments() {
		if segment.GetDbid() == 1 && segment.GetContent() == -1 {
			master = fmt.Sprintf(format, segment.GetDbid(), segment.GetContent(), "MASTER", in.GetHostname())
		} else {
			replies = append(replies, fmt.Sprintf(format, segment.GetDbid(), segment.GetContent(), "PRIMARY", in.GetHostname()))
		}
	}

	replies = append([]string{master}, replies...)

	return &pb.CheckConversionStatusReply{
		Statuses: replies,
	}, nil
}
