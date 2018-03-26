package services_test

import (
	"errors"
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

	It("returns an error when pg_upgrade fails", func() {
		fakeUpgradeConvertMasterRequest := &pb.UpgradeConvertMasterRequest{
			OldBinDir: "/old/path/bin",
			NewBinDir: "/new/path/bin"}

		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errors.New("failed to start"),
		})

		_, err := hub.UpgradeConvertMaster(nil, fakeUpgradeConvertMasterRequest)
		Expect(err).To(HaveOccurred())
	})
})
