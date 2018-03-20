package integrations_test

import (
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	"gp_upgrade/testutils"

	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("upgrade", func() {
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

	XDescribe("share-oids", func() {
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

	Describe("validate-start-cluster", func() {
		It("succeeds if hub is up and gpstart succeeds", func(done Done) {
			defer close(done)
			newBinDir, err := ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())
			newDataDir, err := ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())

			Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Validate the upgraded cluster can start up"))

			trigger := make(chan struct{}, 1)
			commandExecer.SetTrigger(trigger)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()

				Eventually(runStatusUpgrade).Should(ContainSubstring("RUNNING - Validate the upgraded cluster can start up"))
				trigger <- struct{}{}
			}()

			validateStartClusterSession := runCommand("upgrade", "validate-start-cluster", "--new-datadir", newDataDir, "--new-bindir", newBinDir)
			Eventually(validateStartClusterSession).Should(Exit(0))
			wg.Wait()

			Expect(commandExecer.Command()).To(Equal("bash"))
			Expect(strings.Join(commandExecer.Args(), "")).To(ContainSubstring("gpstart"))
			Eventually(runStatusUpgrade).Should(ContainSubstring("COMPLETE - Validate the upgraded cluster can start up"))
		})

		It("fails if hub is not up and gpstart fails", func(done Done) {
			defer close(done)
			newBinDir, err := ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())
			newDataDir, err := ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())

			Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Validate the upgraded cluster can start up"))

			commandExecer.SetOutput(&testutils.FakeCommand{
				Err: errors.New("some exec error"),
			})

			validateStartClusterSession := runCommand("upgrade", "validate-start-cluster", "--new-datadir", newDataDir, "--new-bindir", newBinDir)
			Eventually(validateStartClusterSession).Should(Exit(1))

			Eventually(runStatusUpgrade).Should(ContainSubstring("FAILED - Validate the upgraded cluster can start up"))
		})
	})
})
