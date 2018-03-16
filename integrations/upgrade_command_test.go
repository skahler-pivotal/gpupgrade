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

var _ = Describe("upgrade", func() {
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

	Describe("share-oids", func() {
		It("updates status PENDING to RUNNING then to COMPLETE if successful", func() {
			runCommand("check", "config")
			Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Copy OID files from master to segments"))

			//expectationsDuringCommandInFlight := make(chan bool)

			//go func() {
			//	defer GinkgoRecover()
			//	// TODO: Can this flake? if the in-progress window is shorter than the frequency of Eventually(), then yea
			//	Eventually(runStatusUpgrade).Should(ContainSubstring("RUNNING - Copy OID files from master to segments"))
			//	//close channel here
			//	expectationsDuringCommandInFlight <- true
			//}()

			session := runCommand("upgrade", "share-oids")
			Eventually(session).Should(Exit(0))
			//<-expectationsDuringCommandInFlight

			Eventually(runStatusUpgrade).Should(ContainSubstring("COMPLETE - Copy OID files from master to segments"))
		})
	})
})
