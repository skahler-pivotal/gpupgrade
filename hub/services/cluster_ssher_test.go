package services_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gp_upgrade/hub/services"
	"gp_upgrade/utils"

	"github.com/pkg/errors"
)

var _ = Describe("ClusterSsher", func() {
	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})

	Describe("VerifySoftware", func() {
		It("indicates that it is in progress, failed on the hub filesystem", func() {
			utils.System.ExecCmdCombinedOutput = func(name string, args ...string) ([]byte, error) {
				return nil, errors.New("host not found")
			}
			cw := newSpyChecklistWriter()
			ap := newSpyAgentPinger()
			clusterSsher := services.NewClusterSsher(cw, ap)
			clusterSsher.VerifySoftware([]string{"doesnt matter"})
			Expect(cw.freshStateDirs).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedInProgress).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedFailed).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedCompleted).ToNot(ContainElement("seginstall"))
		})
		It("indicates that it is in progress, completed on the hub filesystem", func() {
			var recvdName string
			var recvdArgs []string
			utils.System.ExecCmdCombinedOutput = func(name string, args ...string) ([]byte, error) {
				recvdName = name
				recvdArgs = args
				return []byte("completed"), nil
			}
			cw := newSpyChecklistWriter()
			ap := newSpyAgentPinger()
			clusterSsher := services.NewClusterSsher(cw, ap)
			clusterSsher.VerifySoftware([]string{"doesnt matter"})
			Expect(recvdName).To(Equal("ssh"))
			Expect(recvdArgs).To(ContainElement("-o"))
			Expect(recvdArgs).To(ContainElement("StrictHostKeyChecking=no"))
			Expect(recvdArgs).To(ContainElement("ls"))
			Expect(recvdArgs[len(recvdArgs)-1]).To(HaveSuffix("gp_upgrade_agent"))
			Expect(cw.freshStateDirs).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedInProgress).To(ContainElement("seginstall"))
			Expect(cw.stepsMarkedFailed).ToNot(ContainElement("seginstall"))
			Expect(cw.stepsMarkedCompleted).To(ContainElement("seginstall"))
		})
	})

	Describe("Start", func() {
		It("starts the agents", func() {
			var recvdName string
			var recvdArgs []string
			utils.System.ExecCmdCombinedOutput = func(name string, args ...string) ([]byte, error) {
				recvdName = name
				recvdArgs = args
				return []byte("completed"), nil
			}
			cw := newSpyChecklistWriter()
			ap := newSpyAgentPinger()
			clusterSsher := services.NewClusterSsher(cw, ap)
			clusterSsher.Start([]string{"doesnt matter"})
			Expect(recvdName).To(Equal("ssh"))
			Expect(recvdArgs).To(ContainElement("-o"))
			Expect(recvdArgs).To(ContainElement("StrictHostKeyChecking=no"))
			Expect(recvdArgs).To(ContainElement("doesnt matter"))
			//the ampersand is a hack until gp_upgrade_agent can background itself
			Expect(recvdArgs[len(recvdArgs)-1]).To(HaveSuffix("gp_upgrade_agent > /dev/null 2>&1 & '"))
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
