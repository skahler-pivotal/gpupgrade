package services_test

import (
	"github.com/greenplum-db/gpupgrade/agent/services"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	"errors"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Agent Check_free_ports", func() {
	BeforeEach(func() {
		testhelper.SetupTestLogger()
	})

	AfterEach(func() {
		//any mocking of utils.System function pointers should be reset by calling InitializeSystemFunctions
		utils.System = utils.InitializeSystemFunctions()
	})

	It("errored out while running netstat", func() {
		errChan := make(chan error, 2)
		outChan := make(chan []byte, 2)
		commandExecer := &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errChan,
			Out: outChan,
		})
		outChan <- []byte("")
		errChan <- errors.New("bash could not run netstat")

		agent := services.NewAgentServer(commandExecer.Exec, services.AgentConfig{})

		_, err := agent.CheckFreePorts(nil, &pb.CheckFreePortsRequest{})
		Expect(err).To(HaveOccurred())
	})

	It("finds that all the ports in the specified range are free", func() {
		errChan := make(chan error, 2)
		outChan := make(chan []byte, 2)
		commandExecer := &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errChan,
			Out: outChan,
		})
		outChan <- []byte("")
		errChan <- nil

		agent := services.NewAgentServer(commandExecer.Exec, services.AgentConfig{})

		reply, err := agent.CheckFreePorts(nil, &pb.CheckFreePortsRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(reply.Result).To(BeTrue())
	})

	It("finds that one or more ports in the specified range are in use", func() {
		errChan := make(chan error, 2)
		outChan := make(chan []byte, 2)
		commandExecer := &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errChan,
			Out: outChan,
		})
		outChan <- []byte("15432")
		errChan <- nil

		agent := services.NewAgentServer(commandExecer.Exec, services.AgentConfig{})

		reply, err := agent.CheckFreePorts(nil, &pb.CheckFreePortsRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(reply.Result).To(BeFalse())
	})

})
