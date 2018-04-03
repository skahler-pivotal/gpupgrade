package services_test

import (
	"errors"
	"time"

	pb "gp_upgrade/idl"
	mockpb "gp_upgrade/mock_idl"

	"github.com/golang/mock/gomock"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub pings agents test", func() {
	var (
		client        *mockpb.MockAgentClient
		ctrl          *gomock.Controller
		pingerManager *services.PingerManager
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mockpb.NewMockAgentClient(ctrl)
		pingerManager = &services.PingerManager{
			[]configutils.ClientAndHostname{{Client: client, Hostname: "doesnotexist"}},
			10,
			1 * time.Millisecond,
		}
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("PingAllAgents", func() {
		It("grpc calls succeed, all agents are running", func() {
			client.EXPECT().PingAgents(
				gomock.Any(),
				&pb.PingAgentsRequest{},
			).Return(&pb.PingAgentsReply{}, nil)

			err := pingerManager.PingAllAgents()
			Expect(err).To(BeNil())
		})

		It("grpc calls fail, not all agents are running", func() {
			client.EXPECT().PingAgents(
				gomock.Any(),
				&pb.PingAgentsRequest{},
			).Return(&pb.PingAgentsReply{}, errors.New("call to agent fail"))

			err := pingerManager.PingAllAgents()
			Expect(err).To(MatchError("call to agent fail"))
		})
	})
	Describe("PingAllAgents", func() {
		It("grpc calls succeed, only one ping", func() {
			client.EXPECT().PingAgents(
				gomock.Any(),
				&pb.PingAgentsRequest{},
			).Return(&pb.PingAgentsReply{}, nil)

			err := pingerManager.PingPollAgents()
			Expect(err).ToNot(HaveOccurred())
		})

		It("grpc calls fail, ping timeout exceeded", func() {
			for i := 0; i < pingerManager.NumRetries; i++ {
				client.EXPECT().PingAgents(
					gomock.Any(),
					&pb.PingAgentsRequest{},
				).Return(&pb.PingAgentsReply{}, errors.New("call to agent fail"))
			}

			err := pingerManager.PingPollAgents()
			Expect(err).To(MatchError("call to agent fail"))
		})
	})
})
