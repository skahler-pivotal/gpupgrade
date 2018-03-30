package testutils

import (
	"context"
	"net"

	pb "gp_upgrade/idl"

	"google.golang.org/grpc"
)

type MockAgentServer struct {
	addr       net.Addr
	grpcServer *grpc.Server

	StatusConversionRequest  *pb.CheckConversionStatusRequest
	StatusConversionResponse *pb.CheckConversionStatusReply
	StatusConversionErr      error
}

func NewMockAgentServer() (*MockAgentServer, int) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}

	mockServer := &MockAgentServer{
		addr:       lis.Addr(),
		grpcServer: grpc.NewServer(),
	}

	pb.RegisterAgentServer(mockServer.grpcServer, mockServer)

	go func() {
		mockServer.grpcServer.Serve(lis)
	}()

	return mockServer, lis.Addr().(*net.TCPAddr).Port
}

func (m *MockAgentServer) CheckUpgradeStatus(context.Context, *pb.CheckUpgradeStatusRequest) (*pb.CheckUpgradeStatusReply, error) {
	return nil, nil
}

func (m *MockAgentServer) CheckConversionStatus(ctx context.Context, in *pb.CheckConversionStatusRequest) (*pb.CheckConversionStatusReply, error) {
	m.StatusConversionRequest = in
	return m.StatusConversionResponse, m.StatusConversionErr
}

func (m *MockAgentServer) CheckDiskUsageOnAgents(context.Context, *pb.CheckDiskUsageRequestToAgent) (*pb.CheckDiskUsageReplyFromAgent, error) {
	return nil, nil
}

func (m *MockAgentServer) PingAgents(context.Context, *pb.PingAgentsRequest) (*pb.PingAgentsReply, error) {
	return nil, nil
}

func (m *MockAgentServer) Stop() {
	m.grpcServer.Stop()
}
