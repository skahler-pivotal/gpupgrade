package integrations_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	"gp_upgrade/testutils"

	"github.com/onsi/gomega/gbytes"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

// the `prepare start-hub` tests are currently in master_only_integration_test
var _ = Describe("prepare", func() {
	var (
		dir           string
		hub           *services.HubClient
		commandExecer *testutils.FakeCommandExecer
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		conf := &services.HubConfig{
			CliToHubPort:   7527,
			HubToAgentPort: 6416,
			StateDir:       dir,
		}
		reader := configutils.NewReader()

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})

		hub = services.NewHub(&cluster.Pair{}, &reader, grpc.DialContext, commandExecer.Exec, conf)

		Expect(checkPortIsAvailable(7527)).To(BeTrue())
		go hub.Start()
	})

	AfterEach(func() {
		hub.Stop()
		Expect(checkPortIsAvailable(7527)).To(BeTrue())
		os.RemoveAll(dir)
	})

	/* This is demonstrating the limited implementation of init-cluster.
	    Assuming the user has already set up their new cluster, they should `init-cluster`
		with the port at which they stood it up, so the upgrade tool can create new_cluster_config

		In the future, the upgrade tool might take responsibility for starting its own cluster,
		in which case it won't need the port, but would still generate new_cluster_config
	*/
	Describe("Given that a gpdb cluster is up, in this case reusing the single cluster for other test.", func() {
		It("can save the database configuration json under the name 'new cluster'", func() {
			statusSessionPending := runCommand("status", "upgrade")
			Eventually(statusSessionPending).Should(gbytes.Say("PENDING - Initialize upgrade target cluster"))

			port := os.Getenv("PGPORT")
			Expect(port).ToNot(Equal(""), "PGPORT needs to be set!")

			session := runCommand("prepare", "init-cluster", "--port", port)

			if session.ExitCode() != 0 {
				fmt.Println("make sure greenplum is running")
			}
			Eventually(session).Should(Exit(0))

			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession).Should(gbytes.Say("COMPLETE - Initialize upgrade target cluster"))

			// check file
			_, err := ioutil.ReadFile(configutils.GetNewClusterConfigFilePath(dir))
			testutils.Check("cannot read file", err)

			reader := configutils.NewReader()
			reader.OfNewClusterConfig(dir)
			err = reader.Read()
			testutils.Check("cannot read config", err)

			// for extra credit, read db and compare info
			Expect(len(reader.GetSegmentConfiguration())).To(BeNumerically(">", 1))
		})
	})
})
