package integrations_test

import (
	"github.com/onsi/gomega/gbytes"
	"gp_upgrade/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("status", func() {
	Describe("conversion", func() {
		It("Displays status information for all segments", func() {
			ensureHubIsUp()
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

			testutils.WriteProvidedConfig(config)

			statusSession := runCommand("status", "conversion")
			Eventually(statusSession).Should(Exit(0))

			Eventually(statusSession).Should(gbytes.Say("PENDING - DBID 1 - CONTENT ID -1 - MASTER - .+"))
			Eventually(statusSession).Should(gbytes.Say("PENDING - DBID [0-9] - CONTENT ID [0-9] - PRIMARY - .+"))
		})
	})

	Describe("upgrade", func() {
		It("Reports some demo status from the hub", func() {
			ensureHubIsUp()
			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession).Should(Exit(0))

			Eventually(statusSession).Should(gbytes.Say("PENDING - Configuration Check"))
			Eventually(statusSession).Should(gbytes.Say("PENDING - Install binaries on segments"))
		})

		// ultimately, the status command isn't uniquely responsible for the cases where the hub is down
		// consider moving this case alongside the `prepare start-hub` integration tests
		It("Explodes if the hub isn't up", func() {
			killHub()
			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession.Err).Should(gbytes.Say("Unable to connect to hub:"))
			Eventually(statusSession).Should(Exit(1))
		})
	})
})
