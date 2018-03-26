package integrations_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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

var _ = Describe("upgrade convert master", func() {
	var (
		dir           string
		hub           *services.HubClient
		commandExecer *testutils.FakeCommandExecer
		oldDataDir    string
		oldBinDir     string
		newDataDir    string
		newBinDir     string
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		oldDataDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		oldBinDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		newDataDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		newBinDir, err = ioutil.TempDir("", "")
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

	It("updates status PENDING to RUNNING then to COMPLETE if successful", func() {
		Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Run pg_upgrade on master"))

		commandExecer.SetOutput(&testutils.FakeCommand{
			Out: []byte("pid1"),
		})

		upgradeConvertMasterSession := runCommand(
			"upgrade",
			"convert-master",
			"--old-datadir", oldDataDir,
			"--old-bindir", oldBinDir,
			"--new-datadir", newDataDir,
			"--new-bindir", newBinDir,
		)
		Expect(upgradeConvertMasterSession).Should(Exit(0))
		Eventually(runStatusUpgrade).Should(ContainSubstring("RUNNING - Run pg_upgrade on master"))
		// Allow new stop to complete

		f, err := os.Create(filepath.Join(dir, "pg_upgrade", "fakeUpgradeFile.done"))
		Expect(err).ToNot(HaveOccurred())
		f.Write([]byte("Upgrade complete\n")) //need t
		f.Close()

		commandExecer.SetOutput(&testutils.FakeCommand{})

		allCalls := strings.Join(commandExecer.Calls(), "")
		Expect(allCalls).To(ContainSubstring(newBinDir + "/pg_upgrade"))

		Expect(runStatusUpgrade()).To(ContainSubstring("COMPLETE - Run pg_upgrade on master"))

	})

	It("updates status to FAILED if it fails to run", func() {
		Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Run pg_upgrade on master"))

		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errors.New("start failed"),
			Out: nil,
		})

		upgradeConvertMasterSession := runCommand(
			"upgrade",
			"convert-master",
			"--old-datadir", oldDataDir,
			"--old-bindir", oldBinDir,
			"--new-datadir", newDataDir,
			"--new-bindir", newBinDir,
		)
		Expect(upgradeConvertMasterSession).Should(Exit(1))

		Expect(runStatusUpgrade()).To(ContainSubstring("FAILED - Run pg_upgrade on master"))
	})

	It("fails if the --old-bindir or --new-bindir flags are missing", func() {
		prepareShutdownClustersSession := runCommand("upgrade", "convert-master")
		Expect(prepareShutdownClustersSession).Should(Exit(1))
		Expect(string(prepareShutdownClustersSession.Out.Contents())).To(Equal("Required flag(s) \"new-bindir\", \"new-datadir\", \"old-bindir\", \"old-datadir\" have/has not been set\n"))
	})
})
