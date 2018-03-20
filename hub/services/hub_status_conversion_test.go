package services_test

import (
	"errors"
	"gp_upgrade/testutils"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"google.golang.org/grpc"

	pb "gp_upgrade/idl"

	"gp_upgrade/hub/services"

	"gp_upgrade/hub/configutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	var (
		hubClient *services.HubClient
		agentA    *testutils.MockAgentServer
	)

	BeforeEach(func() {
		testhelper.SetupTestLogger()
		utils.System = utils.InitializeSystemFunctions()

		agentA = testutils.NewMockAgentServer()

		reader := &spyReader{
			hostnames: []string{"localhost", "localhost"},
			segmentConfiguration: configutils.SegmentConfiguration{
				{
					Content:  0,
					DBID:     2,
					Hostname: "localhost",
				}, {
					Content:  1,
					DBID:     3,
					Hostname: "localhost",
				},
			},
		}

		conf := &services.HubConfig{
			HubToAgentPort: 6416,
		}

		hubClient = services.NewHub(nil, reader, grpc.DialContext, nil, conf)
	})

	AfterEach(func() {
		agentA.Stop()
	})

	It("receives a conversion status from each agent and returns all as single message", func() {
		statusMessages := []string{"status", "status"}
		agentA.StatusConversionResponse = &pb.CheckConversionStatusReply{
			Statuses: statusMessages,
		}

		status, err := hubClient.StatusConversion(nil, &pb.StatusConversionRequest{})
		Expect(err).ToNot(HaveOccurred())

		Expect(status.GetConversionStatuses()).To(Equal([]string{"status", "status", "status", "status"}))
		Expect(agentA.StatusConversionRequest.GetHostname()).To(Equal("localhost"))
		Expect(agentA.StatusConversionRequest.GetSegments()).To(ConsistOf([]*pb.SegmentInfo{
			{
				Content: 0,
				Dbid:    2,
			},
			{
				Content: 1,
				Dbid:    3,
			},
		}))
	})

	It("returns an error when AgentConns returns an error", func() {
		agentA.Stop()

		_, err := hubClient.StatusConversion(nil, &pb.StatusConversionRequest{})
		Expect(err).To(HaveOccurred())
	})

	It("returns an error when Agent server returns an error", func() {
		agentA.StatusConversionErr = errors.New("any error")

		_, err := hubClient.StatusConversion(nil, &pb.StatusConversionRequest{})
		Expect(err).To(HaveOccurred())
	})
})
