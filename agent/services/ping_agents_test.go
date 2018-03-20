package services_test

import (
	"gp_upgrade/agent/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("CommandListener", func() {
	var (
		testLogFile *gbytes.Buffer
	)

	BeforeEach(func() {
		_, _, testLogFile = testhelper.SetupTestLogger()
	})

	AfterEach(func() {
		//any mocking of utils.System function pointers should be reset by calling InitializeSystemFunctions
		utils.System = utils.InitializeSystemFunctions()
	})

	It("returns an empty reply", func() {
		commandExecer := &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})

		agent := services.NewAgentServer(commandExecer.Exec)

		_, err := agent.PingAgents(nil, &pb.PingAgentsRequest{})
		Expect(err).To(BeNil())
	})
})
