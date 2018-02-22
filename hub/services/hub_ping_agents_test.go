package services_test

import (
	"errors"
	pb "gp_upgrade/idl"
	mockpb "gp_upgrade/mock_idl"

	"github.com/golang/mock/gomock"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("hub pings agents test", func() {
	var (
		client        *mockpb.MockCommandListenerClient
		ctrl          *gomock.Controller
		testLogFile   *gbytes.Buffer
		pingerManager *services.PingerManager
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mockpb.NewMockCommandListenerClient(ctrl)
		_, _, testLogFile = testhelper.SetupTestLogger()
		pingerManager = &services.PingerManager{
			[]configutils.ClientAndHostname{{Client: client, Hostname: "doesnotexist"}},
			10,
		}
	})

	AfterEach(func() {
		defer ctrl.Finish()
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
			Expect(err.Error()).To(ContainSubstring("call to agent fail"))
			Expect(string(testLogFile.Contents())).To(ContainSubstring("Not all agents on the segment hosts are running"))
		})
	})
	Describe("PingAllAgents", func() {
		It("grpc calls succeed, only one ping", func() {
			client.EXPECT().PingAgents(
				gomock.Any(),
				&pb.PingAgentsRequest{},
			).Return(&pb.PingAgentsReply{}, nil)

			err := pingerManager.PingPollAgents()
			Expect(err).To(BeNil())
			logContents := string(testLogFile.Contents())
			Expect(logContents).To(ContainSubstring("Pinging agents..."))
			Expect(logContents).To(Not(ContainSubstring("Not all agents on the segment hosts are running.")))
			Expect(logContents).To(Not(ContainSubstring("Reached ping timeout")))
		})
		It("grpc calls fail, ping timeout exceeded", func() {
			for i := 0; i < pingerManager.NumRetries; i++ {
				client.EXPECT().PingAgents(
					gomock.Any(),
					&pb.PingAgentsRequest{},
				).Return(&pb.PingAgentsReply{}, errors.New("call to agent fail"))
			}

			err := pingerManager.PingPollAgents()
			Expect(err.Error()).To(ContainSubstring("call to agent fail"))
			logContents := string(testLogFile.Contents())
			Expect(logContents).To(ContainSubstring("Reached ping timeout"))
			Expect(logContents).To(MatchRegexp(`Pinging agents\.\.\.\n.*Not all agents on the segment hosts are running\.\n.*Pinging agents\.\.\.\n`))
		})
	})
})
