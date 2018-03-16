package integrations_test

import (
	"io/ioutil"
	"os"

	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"google.golang.org/grpc"
)

// needs the cli and the hub
var _ = Describe("prepare", func() {
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

		runCommand("check", "config")
	})

	AfterEach(func() {
		hub.Stop()
		Expect(checkPortIsAvailable(7527)).To(BeTrue())
		os.RemoveAll(dir)
	})

	// TODO: This test might be interesting to run multi-node; for that, figure out how "installation" should be done
	Describe("start-agents", func() {
		// afiak the reason done Done is not used here is because the we are waiting for both the go routine and the
		// main thread to finish. Calling close(done) could theoretically end the test early.
		It("updates status PENDING to RUNNING then to COMPLETE if successful", func() {
			Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Agents Started on Cluster"))
			expectationsDuringCommandInFlight := make(chan bool)
			go func() {
				defer GinkgoRecover()
				// TODO: Can this flake? if the in-progress window is shorter than the frequency of Eventually(), then yea
				Eventually(runStatusUpgrade).Should(ContainSubstring("RUNNING - Agents Started on Cluster"))
				expectationsDuringCommandInFlight <- true
			}()

			session := runCommand("prepare", "start-agents")
			Eventually(session).Should(Exit(0))
			<-expectationsDuringCommandInFlight
			Eventually(runStatusUpgrade, 3).Should(ContainSubstring("COMPLETE - Agents Started on Cluster"))
		})
	})

})
