package services_test

import (
	"errors"
	"strings"

	"gp_upgrade/testutils"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"google.golang.org/grpc"

	pb "gp_upgrade/idl"

	"gp_upgrade/hub/services"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gp_upgrade/hub/configutils"
)

var _ = Describe("hub", func() {
	var (
		hubClient   *services.HubClient
		shutdownHub func()
		agentA      *testutils.MockAgentServer
	)

	BeforeEach(func() {
		testhelper.SetupTestLogger()
		utils.System = utils.InitializeSystemFunctions()

		agentA = testutils.NewMockAgentServer()

		reader := spyReader{}
		reader.hostnames = []string{"localhost", "localhost"}
		reader.segmentConfiguration = configutils.SegmentConfiguration{
			{
				Content:  0,
				DBID:     2,
				Hostname: "localhost",
			}, {
				Content:  1,
				DBID:     3,
				Hostname: "localhost",
			},
		}

		hubClient, shutdownHub = services.NewHub(nil, &reader, grpc.DialContext)
	})

	AfterEach(func() {
		shutdownHub()

		agentA.Stop()
	})

	It("receives a conversion status from each agent and returns all as single message", func() {
		statusMessages := []string{"status", "status"}
		agentA.StatusConversionResponse = &pb.CheckConversionStatusReply{
			Statuses: statusMessages,
		}

		status, err := hubClient.StatusConversion(nil, &pb.StatusConversionRequest{})
		Expect(err).ToNot(HaveOccurred())

		statusList := strings.Split(status.GetConversionStatus(), "\n")
		Expect(statusList).To(Equal([]string{"status", "status", "status", "status"}))
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
