package integrations_test

import (
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gpupgrade/hub/cluster"
	"github.com/greenplum-db/gpupgrade/hub/configutils"
	"github.com/greenplum-db/gpupgrade/hub/services"
	"github.com/greenplum-db/gpupgrade/testutils"

	"github.com/onsi/gomega/gbytes"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"github.com/greenplum-db/gpupgrade/hub/cluster_ssher"
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	"time"
)

// the `prepare start-hub` tests are currently in master_only_integration_test
var _ = Describe("prepare init-cluster", func() {
	var (
		dir           string
		hub           *services.Hub
		commandExecer *testutils.FakeCommandExecer
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		port, err = testutils.GetOpenPort()
		Expect(err).ToNot(HaveOccurred())

		conf := &services.HubConfig{
			CliToHubPort:   port,
			HubToAgentPort: 6416,
			StateDir:       dir,
		}
		reader := configutils.NewReader()

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})

		clusterSsher := cluster_ssher.NewClusterSsher(
			upgradestatus.NewChecklistManager(conf.StateDir),
			services.NewPingerManager(conf.StateDir, 500*time.Millisecond),
			commandExecer.Exec,
		)
		hub = services.NewHub(&cluster.Pair{}, &reader, grpc.DialContext, commandExecer.Exec, conf, clusterSsher)
		go hub.Start()
	})

	AfterEach(func() {
		hub.Stop()
		os.RemoveAll(dir)
		Expect(checkPortIsAvailable(port)).To(BeTrue())
	})

	It("starts the target cluster, and generates a new_cluster_config.json", func() {
		statusSessionPending := runCommand("status", "upgrade")
		Eventually(statusSessionPending).Should(gbytes.Say("PENDING - Initialize upgrade target cluster"))

		checkConfigSession := runCommand("check", "config", "--master-host", "localhost")
		Eventually(checkConfigSession).Should(Exit(0))

		agentSession := runCommand("prepare", "start-agents")
		Eventually(agentSession).Should(Exit(0))

		prepareSession := runCommand("prepare", "init-cluster")
		Eventually(prepareSession).Should(Exit(0))

		Expect(runStatusUpgrade()).To(ContainSubstring("COMPLETE - Initialize upgrade target cluster"))

			session := runCommand("prepare", "init-cluster", "--port", port, "--new-bindir", "/tmp")
			Eventually(session).Should(Exit(0))

			Expect(runStatusUpgrade()).To(ContainSubstring("COMPLETE - Initialize upgrade target cluster"))

			reader := configutils.NewReader()
			reader.OfNewClusterConfig(dir)
			err := reader.Read()
			Expect(err).ToNot(HaveOccurred())

			Expect(len(reader.GetSegmentConfiguration())).To(BeNumerically(">", 1))
		})
	})

	It("fails if some flags are missing", func() {
		prepareStartAgentsSession := runCommand("prepare", "init-cluster")
		Expect(prepareStartAgentsSession).Should(Exit(1))
		Expect(string(prepareStartAgentsSession.Out.Contents())).To(Equal("Required flag(s) \"new-bindir\", \"port\" have/has not been set\n"))
	})
})
