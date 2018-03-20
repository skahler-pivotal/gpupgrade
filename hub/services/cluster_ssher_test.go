package services_test

import (
	"os"
	"path/filepath"

	"gp_upgrade/testutils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gp_upgrade/hub/services"
	"gp_upgrade/utils"

	"github.com/pkg/errors"
)

var _ = Describe("ClusterSsher", func() {
	BeforeEach(func() {
		testhelper.SetupTestLogger()
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})

	Describe("VerifySoftware", func() {
		It("indicates that it is in progress, failed on the hub filesystem", func() {
			commandExecer := &testutils.FakeCommandExecer{}
			commandExecer.SetOutput(&testutils.FakeCommand{
				Out: []byte("stdout/stderr message"),
				Err: errors.New("host not found"),
			})

			cw := newSpyChecklistWriter()
			clusterSsher := services.NewClusterSsher(cw, newSpyAgentPinger(), commandExecer.Exec)
			clusterSsher.VerifySoftware([]string{"doesnt matter"})

			Expect(cw.freshStateDirs).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedInProgress).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedFailed).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedCompleted).ToNot(ContainElement("seginstall"))
		})
		It("indicates that it is in progress, completed on the hub filesystem", func() {
			commandExecer := &testutils.FakeCommandExecer{}
			commandExecer.SetOutput(&testutils.FakeCommand{
				Out: []byte("completed"),
				Err: nil,
			})

			cw := newSpyChecklistWriter()
			clusterSsher := services.NewClusterSsher(cw, newSpyAgentPinger(), commandExecer.Exec)
			clusterSsher.VerifySoftware([]string{"doesnt matter"})

			Expect(commandExecer.Command()).To(Equal("ssh"))
			pathToAgent := filepath.Join(os.Getenv("GPHOME"), "bin", "gp_upgrade_agent")
			Expect(commandExecer.Args()).To(Equal([]string{
				"-o",
				"StrictHostKeyChecking=no",
				"doesnt matter",
				"ls",
				pathToAgent,
			}))

			Expect(cw.freshStateDirs).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedInProgress).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedFailed).ToNot(ContainElement("seginstall"))
			Expect(cw.stepsMarkedCompleted).To(ContainElement("seginstall"))
		})
	})

	Describe("Start", func() {
		It("starts the agents", func() {
			commandExecer := &testutils.FakeCommandExecer{}
			commandExecer.SetOutput(&testutils.FakeCommand{
				Out: []byte("stdout/stderr message"),
				Err: errors.New("host not found"),
			})

			cw := newSpyChecklistWriter()
			clusterSsher := services.NewClusterSsher(cw, newSpyAgentPinger(), commandExecer.Exec)
			clusterSsher.Start([]string{"doesnt matter"})

			Expect(commandExecer.Command()).To(Equal("ssh"))
			pathToAgent := filepath.Join(os.Getenv("GPHOME"), "bin", "gp_upgrade_agent")
			Expect(commandExecer.Args()).To(Equal([]string{
				"-o",
				"StrictHostKeyChecking=no",
				"doesnt matter",
				"sh -c 'nohup " + pathToAgent + " > /dev/null 2>&1 & '",
			}))

			Expect(cw.freshStateDirs).To(ContainElement("start-agents"))
			Expect(cw.stepsMarkedInProgress).To(ContainElement("start-agents"))
			Expect(cw.stepsMarkedFailed).ToNot(ContainElement("start-agents"))
			Expect(cw.stepsMarkedCompleted).To(ContainElement("start-agents"))
		})
	})
})

type spyAgentPinger struct{}

func newSpyAgentPinger() *spyAgentPinger {
	return &spyAgentPinger{}
}

func (s *spyAgentPinger) PingPollAgents() error {
	return nil
}

type spyChecklistWriter struct {
	freshStateDirs        []string
	stepsMarkedInProgress []string
	stepsMarkedFailed     []string
	stepsMarkedCompleted  []string
}

func newSpyChecklistWriter() *spyChecklistWriter {
	return &spyChecklistWriter{}
}

func (s *spyChecklistWriter) MarkFailed(step string) error {
	s.stepsMarkedFailed = append(s.stepsMarkedFailed, step)
	return nil
}

func (s *spyChecklistWriter) MarkComplete(step string) error {
	s.stepsMarkedCompleted = append(s.stepsMarkedCompleted, step)
	return nil
}

func (s *spyChecklistWriter) MarkInProgress(step string) error {
	s.stepsMarkedInProgress = append(s.stepsMarkedInProgress, step)
	return nil
}

func (s *spyChecklistWriter) ResetStateDir(step string) error {
	s.freshStateDirs = append(s.freshStateDirs, step)
	return nil
}
