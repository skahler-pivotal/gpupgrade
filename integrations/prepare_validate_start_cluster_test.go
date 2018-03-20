package integrations_test

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	"gp_upgrade/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var _ = Describe("prepare validate-start-cluster", func() {
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

	It("updates status PENDING to RUNNING then to COMPLETE if successful", func(done Done) {
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

		prepareStartAgentsSession := runCommand("upgrade", "validate-start-cluster", "--new-bindir", newBinDir, "--new-datadir", newDataDir)
		Eventually(prepareStartAgentsSession).Should(Exit(0))
		wg.Wait()

		Expect(commandExecer.Command()).To(Equal("bash"))
		Expect(strings.Join(commandExecer.Args(), "")).To(ContainSubstring("gpstart"))
		Eventually(runStatusUpgrade).Should(ContainSubstring("COMPLETE - Validate the upgraded cluster can start up"))
	})

	It("updates status to FAILED if it fails to run", func() {
		newBinDir, err := ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		newDataDir, err := ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Validate the upgraded cluster can start up"))

		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errors.New("start failed"),
			Out: nil,
		})

		prepareStartAgentsSession := runCommand("upgrade", "validate-start-cluster", "--new-bindir", newBinDir, "--new-datadir", newDataDir)
		Eventually(prepareStartAgentsSession).Should(Exit(1))

		Expect(commandExecer.Command()).To(Equal("bash"))
		Expect(strings.Join(commandExecer.Args(), "")).To(ContainSubstring("gpstart"))
		Eventually(runStatusUpgrade).Should(ContainSubstring("FAILED - Validate the upgraded cluster can start up"))
	})
})
