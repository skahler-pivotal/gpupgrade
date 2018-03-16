package integrations_test

import (
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

var _ = Describe("status", func() {
	var (
		dir string
		hub *services.HubClient
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
		hub = services.NewHub(&cluster.Pair{}, &reader, grpc.DialContext, conf)

		Expect(checkPortIsAvailable(7527)).To(BeTrue())
		go hub.Start()
	})

	AfterEach(func() {
		hub.Stop()
		Expect(checkPortIsAvailable(7527)).To(BeTrue())
		os.RemoveAll(dir)
	})

	Describe("conversion", func() {
		It("Displays status information for all segments", func() {
			ensureAgentIsUp()

			config := `[{
  			  "content": 2,
  			  "dbid": 7,
  			  "hostname": "localhost"
  			},
  			{
  			  "content": -1,
  			  "dbid": 1,
  			  "hostname": "localhost"
  			}]`

			testutils.WriteProvidedConfig(dir, config)

			statusSession := runCommand("status", "conversion")
			Eventually(statusSession).Should(Exit(0))

			Eventually(statusSession).Should(gbytes.Say("PENDING - DBID 1 - CONTENT ID -1 - MASTER - .+"))
			Eventually(statusSession).Should(gbytes.Say("PENDING - DBID [0-9] - CONTENT ID [0-9] - PRIMARY - .+"))
		})
	})

	Describe("upgrade", func() {
		It("Reports some demo status from the hub", func() {
			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession).Should(Exit(0))

			Eventually(statusSession).Should(gbytes.Say("PENDING - Configuration Check"))
			Eventually(statusSession).Should(gbytes.Say("PENDING - Install binaries on segments"))
		})

		// ultimately, the status command isn't uniquely responsible for the cases where the hub is down
		// consider moving this case alongside the `prepare start-hub` integration tests
		XIt("Explodes if the hub isn't up", func() {
			//killHub()
			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession.Err).Should(gbytes.Say("Unable to connect to hub:"))
			Eventually(statusSession).Should(Exit(1))
		})
	})
})
