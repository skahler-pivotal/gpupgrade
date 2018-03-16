package integrations_test

import (
	"os"
	"os/exec"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("integration tests running on master only", func() {
	BeforeEach(func() {
		killCommand := exec.Command("pkill", "-9", "gp_upgrade_hub")
		Start(killCommand, GinkgoWriter, GinkgoWriter)

		Expect(checkPortIsAvailable(7527)).To(BeTrue())
	})

	AfterEach(func() {
		killCommand := exec.Command("pkill", "-9", "gp_upgrade_hub")
		Start(killCommand, GinkgoWriter, GinkgoWriter)

		Expect(checkPortIsAvailable(7527)).To(BeTrue())
	})

	Describe("start-hub", func() {
		It("finds the right hub binary and starts a daemonized process", func() {
			gpUpgradeSession := runCommand("prepare", "start-hub")
			Eventually(gpUpgradeSession).Should(Exit(0))

			verificationCmd := exec.Command("bash", "-c", `ps -ef | grep -q "[g]p_upgrade_hub"`)
			verificationSession, err := Start(verificationCmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			Eventually(verificationSession).Should(Exit(0))
		})

		It("returns error if gp_upgrade_hub is already running", func() {
			firstSession := runCommand("prepare", "start-hub")
			Eventually(firstSession).Should(Exit(0))
			//second start should return error
			secondSession := runCommand("prepare", "start-hub")
			Eventually(secondSession).Should(Exit(1))
		})

		It("errs out if doesn't find gp_upgrade_hub on the path", func() {
			origPath := os.Getenv("PATH")
			os.Setenv("PATH", "")
			gpUpgradeSession := runCommand("prepare", "start-hub")
			Eventually(gpUpgradeSession).ShouldNot(Exit(0))
			os.Setenv("PATH", origPath)
		})
	})
})
