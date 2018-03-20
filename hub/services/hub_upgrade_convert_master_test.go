package services_test

import (
	"io/ioutil"
	"path/filepath"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	var (
		mockedOutput     string
		mockedExitStatus int

		testStdout    *gbytes.Buffer
		testStdErr    *gbytes.Buffer
		dir           string
		commandExecer *testutils.FakeCommandExecer
		hub           *services.HubClient
	)

	BeforeEach(func() {
		testStdout, testStdErr, _ = testhelper.SetupTestLogger()
		utils.System = utils.InitializeSystemFunctions()

		reader := configutils.NewReader()

		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		conf := &services.HubConfig{
			StateDir: dir,
		}

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})
		hub = services.NewHub(nil, &reader, grpc.DialContext, commandExecer.Exec, conf)
	})

	Describe("ConvertMasterHub", func() {
		It("Sends that convert master started successfully", func() {
			services.GetMasterDataDirs = func(baseDir string) (string, string, error) {
				return "old/datadirectory/path", "new/datadirectory/path", nil
			}

			fakeUpgradeConvertMasterRequest := &pb.UpgradeConvertMasterRequest{
				OldBinDir: "/old/path/bin",
				NewBinDir: "/new/path/bin"}

			_, err := hub.UpgradeConvertMaster(nil, fakeUpgradeConvertMasterRequest)
			Expect(err).ToNot(HaveOccurred())

			pgupgrade_dir := filepath.Join(dir, "pg_upgrade")
			Expect(commandExecer.Command()).To(Equal("bash"))
			Expect(commandExecer.Args()).To(Equal([]string{
				"-c",
				"unset PGHOST; unset PGPORT; cd " + pgupgrade_dir +
					` && nohup /new/path/bin/pg_upgrade --old-bindir=/old/path/bin --old-datadir=old/datadirectory/path --new-bindir=/new/path/bin --new-datadir=new/datadirectory/path --dispatcher-mode --progress`,
			}))
		})

		// This can't work because we don't have a good way to force a failure
		// for Start? Will need to find a good way.
		XIt("Sends a failure when pg_upgrade failed due to some issue", func() {
			mockedExitStatus = 1
			mockedOutput = `pg_upgrade exploded!
	Some kind of error message here that helps us understand what's going on
	Some kind of obscure error message`

			fakeUpgradeConvertMasterRequest := &pb.UpgradeConvertMasterRequest{
				OldBinDir: "/old/path/bin",
				NewBinDir: "/new/path/bin"}

			_, err := hub.UpgradeConvertMaster(nil, fakeUpgradeConvertMasterRequest)

			Eventually(testStdout).Should(gbytes.Say("Starting master upgrade"))
			Eventually(testStdout).Should(Not(gbytes.Say("Found no errors when starting the upgrade")))

			Eventually(testStdErr).Should(gbytes.Say("An error occured:"))
			Expect(err).To(BeNil())
		})
	})
})
