package services_test

import (
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"gp_upgrade/agent/services"

	"github.com/onsi/gomega/gbytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CommandListener", func() {
	var (
		testLogFile   *gbytes.Buffer
		agent         *services.AgentServer
		commandExecer *testutils.FakeCommandExecer
	)

	BeforeEach(func() {
		_, _, testLogFile = testhelper.SetupTestLogger()

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})

		agent = services.NewAgentServer(commandExecer.Exec)
	})

	AfterEach(func() {
		//any mocking of utils.System function pointers should be reset by calling InitializeSystemFunctions
		utils.System = utils.InitializeSystemFunctions()
	})

	It("returns a status string for each DBID passed from the hub", func() {
		request := &pb.CheckConversionStatusRequest{
			Segments: []*pb.SegmentInfo{{
				Content: 1,
				Dbid:    3,
			}, {
				Content: -1,
				Dbid:    1,
			}},
			Hostname: "localhost",
		}

		status, err := agent.CheckConversionStatus(nil, request)
		Expect(err).ToNot(HaveOccurred())

		Expect(status.GetStatuses()).To(Equal([]string{
			"PENDING - DBID 1 - CONTENT ID -1 - MASTER - localhost",
			"PENDING - DBID 3 - CONTENT ID 1 - PRIMARY - localhost",
		}))
	})

	It("returns an error if no segments are passed", func() {
		request := &pb.CheckConversionStatusRequest{
			Segments: []*pb.SegmentInfo{},
			Hostname: "localhost",
		}

		_, err := agent.CheckConversionStatus(nil, request)
		Expect(err).To(HaveOccurred())
	})
})
