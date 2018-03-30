package integrations_test

import (
	"io/ioutil"
	"os"
	"strings"

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

		config := `[{
			  "datadir": "/some/data/dir",
			  "content": -1,
			  "dbid": 1,
			  "hostname": "localhost",
			  "port": 5432
			}]`

		testutils.WriteOldConfig(dir, config)
		testutils.WriteNewConfig(dir, config)

		port, err = testutils.GetOpenPort()
		Expect(err).ToNot(HaveOccurred())

		conf := &services.HubConfig{
			CliToHubPort:   port,
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
		go hub.Start()
	})

	AfterEach(func() {
		hub.Stop()
		os.RemoveAll(dir)
		commandExecer = nil
		hub = nil
	})

	It("updates status PENDING to RUNNING then to COMPLETE if successful", func(done Done) {
		defer close(done)
		oldBinDir := "/tmpOld"
		newBinDir := "/tmpNew"

		Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Shutdown clusters"))

		trigger := make(chan struct{}, 1)
		commandExecer.SetOutput(&testutils.FakeCommand{
			Out:     outChan,
			Err:     errChan,
			Trigger: trigger,
		})
		outChan <- []byte("pid1")
		trigger <- struct{}{}

		prepareShutdownClustersSession := runCommand("prepare", "shutdown-clusters", "--old-bindir", oldBinDir, "--new-bindir", newBinDir)
		Eventually(prepareShutdownClustersSession).Should(Exit(0))

		Expect(runStatusUpgrade()).To(ContainSubstring("RUNNING - Shutdown clusters"))
		trigger <- struct{}{}

		allCalls := strings.Join(commandExecer.Calls(), "")
		Expect(allCalls).To(ContainSubstring(oldBinDir + "/gpstop -a"))
		Expect(allCalls).To(ContainSubstring(newBinDir + "/gpstop -a"))
		Expect(runStatusUpgrade()).To(ContainSubstring("COMPLETE - Shutdown clusters"))
	})

	It("updates status to FAILED if it fails to run", func() {
		oldBinDir := "/tmpOld"
		newBinDir := "/tmpNew"

		commandExecer.SetOutput(&testutils.FakeCommand{
			Out: outChan,
			Err: errChan,
		})
		Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Shutdown clusters"))

		errChan <- errors.New("start failed")

		prepareShutdownClustersSession := runCommand("prepare", "shutdown-clusters", "--old-bindir", oldBinDir, "--new-bindir", newBinDir)
		Eventually(prepareShutdownClustersSession).Should(Exit(0))

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
