package integrations_test

import (
	"gp_upgrade/cli/commanders"
	"gp_upgrade/testutils"

	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

func TestCommands(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Tests Suite")
}

var (
	cliBinaryPath            string
	hubBinaryPath            string
	agentBinaryPath          string
	sshd                     *exec.Cmd
	fixture_path             string
	sshdPath                 string
	userPreviousPathVariable string
)

var _ = BeforeSuite(func() {
	var err error
	cliBinaryPath, err = Build("gp_upgrade/cli") // if you want build flags, do a separate Build() in a specific integration test
	Expect(err).NotTo(HaveOccurred())

	hubBinaryPath, err = Build("gp_upgrade/hub")
	Expect(err).NotTo(HaveOccurred())
	hubDirectoryPath := path.Dir(hubBinaryPath)

	agentBinaryPath, err = Build("gp_upgrade/agent")
	Expect(err).NotTo(HaveOccurred())
	// move the agent binary into the hub directory and rename to match expected name
	renamedAgentBinaryPath := filepath.Join(hubDirectoryPath, "/gp_upgrade_agent")
	err = os.Rename(agentBinaryPath, renamedAgentBinaryPath)
	Expect(err).NotTo(HaveOccurred())

	// hub gets built as "hub", but rename for integration tests that expect
	// "gp_upgrade_hub" to be on the path
	renamedHubBinaryPath := hubDirectoryPath + "/gp_upgrade_hub"
	err = os.Rename(hubBinaryPath, renamedHubBinaryPath)
	Expect(err).NotTo(HaveOccurred())
	hubBinaryPath = renamedHubBinaryPath

	// put the gp_upgrade_hub on the path don't need to rename the cli nor put
	// it on the path: integration tests should use RunCommand() below
	userPreviousPathVariable = os.Getenv("PATH")
	os.Setenv("PATH", hubDirectoryPath+":"+userPreviousPathVariable)

	sshdPath, err = Build("gp_upgrade/integrations/sshd")
	Expect(err).NotTo(HaveOccurred())

	/* Tests that need a hub up in a specific home directory should start their
	* own. Other tests don't need a hub; don't start a fresh one automatically
	* because it might be a waste. */
	killHub()

	_, this_file_path, _, _ := runtime.Caller(0)
	fixture_path = path.Join(path.Dir(this_file_path), "fixtures")
})

var _ = AfterSuite(func() {
	/* for a developer who runs `make integration` and then goes on to manually
	* test things out they should start their own up under a different HOME dir
	* setting than what ginkgo has been using */
	killHub()
	killAllAgents()

	CleanupBuildArtifacts()
})

var _ = BeforeEach(func() {
	testutils.EnsureHomeDirIsTempAndClean()
})

func runCommand(args ...string) *Session {
	// IMPORTANT TEST INFO: exec.Command forks and runs in a separate process,
	// which has its own Golang context; any mocks/fakes you set up in
	// the test context will NOT be meaningful in the new exec.Command context.
	cmd := exec.Command(cliBinaryPath, args...)
	session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())
	<-session.Exited

	return session
}

func ensureHubIsUp() {
	countHubs, _ := commanders.HowManyHubsRunning()

	if countHubs == 0 {
		cmd := exec.Command("gp_upgrade_hub", "&")
		Start(cmd, GinkgoWriter, GinkgoWriter)
	}
}

func killHub() {
	//pkill gp_upgrade_ will kill both gp_upgrade_hub and gp_upgrade_agent
	pkillCmd := exec.Command("pkill", "gp_upgrade_hub")
	pkillCmd.Run()
}

func ensureAgentIsUp() {
	killAllAgents()

	cmd := exec.Command("gp_upgrade_agent", "&")
	Start(cmd, GinkgoWriter, GinkgoWriter)
}

func killAllAgents() {
	pkillCmd := exec.Command("pkill", "gp_upgrade_agent")
	pkillCmd.Run()
}
