package integrations_test

import (
	"fmt"
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

var _ = Describe("prepare shutdown-clusters", func() {
	var (
		dir           string
		hub           *services.HubClient
		commandExecer *testutils.FakeCommandExecer
		outChan       chan []byte
		errChan       chan error
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		port := os.Getenv("PGPORT")
		Expect(port).ToNot(Equal(""), "PGPORT needs to be set!")

		dataDir := os.Getenv("MASTER_DATA_DIRECTORY")
		Expect(port).ToNot(Equal(""), "MASTER_DATA_DIRECTORY needs to be set!")

		config := fmt.Sprintf(`[{
  			  "content": -1,
  			  "dbid": 1,
  			  "hostname": "localhost",
              "datadir": "%s",
              "mode": "s",
              "preferred_role": "m",
              "role": "m",
              "status": "u",
			  "port": %s
  			}]`, dataDir, port)

		testutils.WriteProvidedConfig(dir, config)
		testutils.WriteNewProvidedConfig(dir, config)

		conf := &services.HubConfig{
			CliToHubPort:   7527,
			HubToAgentPort: 6416,
			StateDir:       dir,
		}
		reader := configutils.NewReader()

		outChan = make(chan []byte, 2)
		errChan = make(chan error, 2)

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{
			Out: outChan,
			Err: errChan,
		})

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
		oldBinDir, err := ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		newBinDir, err := ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Shutdown clusters"))

		trigger := make(chan struct{}, 10)
		commandExecer.SetOutput(&testutils.FakeCommand{
			Out:     outChan,
			Err:     errChan,
			Trigger: trigger,
		})
		outChan <- []byte("pid1")

		// Trigger for old stop so an in.progress file is written
		trigger <- struct{}{}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer GinkgoRecover()

			Eventually(runStatusUpgrade).Should(ContainSubstring("RUNNING - Shutdown clusters"))
			// Allow new stop to complete
			trigger <- struct{}{}
		}()

		prepareShutdownClustersSession := runCommand("prepare", "shutdown-clusters", "--old-bindir", oldBinDir, "--new-bindir", newBinDir)
		Eventually(prepareShutdownClustersSession).Should(Exit(0))
		wg.Wait()

		allCalls := strings.Join(commandExecer.Calls(), "")
		Expect(allCalls).To(ContainSubstring(oldBinDir + "/gpstop -a"))
		Expect(allCalls).To(ContainSubstring(newBinDir + "/gpstop -a"))
		Eventually(runStatusUpgrade).Should(ContainSubstring("COMPLETE - Shutdown clusters"))
	})

	It("updates status to FAILED if it fails to run", func() {
		oldBinDir, err := ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		newBinDir, err := ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Shutdown clusters"))

		errChan <- errors.New("start failed")

		prepareShutdownClustersSession := runCommand("prepare", "shutdown-clusters", "--old-bindir", oldBinDir, "--new-bindir", newBinDir)
		Expect(prepareShutdownClustersSession).Should(Exit(0))

		allCalls := strings.Join(commandExecer.Calls(), "")
		Expect(allCalls).To(ContainSubstring(oldBinDir + "/gpstop -a"))
		Expect(allCalls).To(ContainSubstring(newBinDir + "/gpstop -a"))
		Eventually(runStatusUpgrade).Should(ContainSubstring("FAILED - Shutdown clusters"))
	})

	It("fails if the --old-bindir or --new-bindir flags are missing", func() {
		prepareShutdownClustersSession := runCommand("prepare", "shutdown-clusters")
		Expect(prepareShutdownClustersSession).Should(Exit(1))
		Expect(string(prepareShutdownClustersSession.Out.Contents())).To(Equal("Required flag(s) \"new-bindir\", \"old-bindir\" have/has not been set\n"))
	})
})
