package services_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	"gp_upgrade/hub/upgradestatus"
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strings"
	"sync"
)

var _ = Describe("upgrade validate start cluster", func() {
	var (
		hub           *services.HubClient
		testStdout    *gbytes.Buffer
		reader        configutils.Reader
		dir           string
		commandExecer *testutils.FakeCommandExecer
	)

	BeforeEach(func() {
		reader = configutils.NewReader()
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})

		hub = services.NewHub(nil, &reader, grpc.DialContext, commandExecer.Exec, &services.HubConfig{
			StateDir: dir,
		})

		testStdout, _, _ = testhelper.SetupTestLogger()
	})

	AfterEach(func() {
		os.RemoveAll(dir)
	})

	It("sets status to COMPLETE when validate start cluster request has been made and returns no error", func() {
		stateChecker := upgradestatus.NewStateCheck(
			filepath.Join(dir, "validate-start-cluster"),
			pb.UpgradeSteps_VALIDATE_START_CLUSTER,
		)

		trigger := make(chan struct{}, 1)
		commandExecer.SetTrigger(trigger)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer GinkgoRecover()

			Eventually(stateChecker.GetStatus).Should(Equal(&pb.UpgradeStepStatus{
				Step:   pb.UpgradeSteps_VALIDATE_START_CLUSTER,
				Status: pb.StepStatus_RUNNING,
			}))
			trigger <- struct{}{}
		}()

		_, err := hub.UpgradeValidateStartCluster(nil, &pb.UpgradeValidateStartClusterRequest{
			NewBinDir:  "bin",
			NewDataDir: "data",
		})
		Expect(err).ToNot(HaveOccurred())
		wg.Wait()

		Expect(commandExecer.Command()).To(Equal("bash"))
		Expect(strings.Join(commandExecer.Args(), "")).To(ContainSubstring("PYTHONPATH="))
		Expect(strings.Join(commandExecer.Args(), "")).To(ContainSubstring("&& bin/gpstart -a -d data"))

		Eventually(stateChecker.GetStatus).Should(Equal(&pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_VALIDATE_START_CLUSTER,
			Status: pb.StepStatus_COMPLETE,
		}))
	})

	It("sets status to FAILED when the validate start cluster request returns an error", func() {
		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errors.New("some error"),
		})

		_, err := hub.UpgradeValidateStartCluster(nil, &pb.UpgradeValidateStartClusterRequest{
			NewBinDir:  "bin",
			NewDataDir: "data",
		})
		Expect(err).To(HaveOccurred())

		stateChecker := upgradestatus.NewStateCheck(
			filepath.Join(dir, "validate-start-cluster"),
			pb.UpgradeSteps_VALIDATE_START_CLUSTER,
		)

		Eventually(stateChecker.GetStatus).Should(Equal(&pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_VALIDATE_START_CLUSTER,
			Status: pb.StepStatus_FAILED,
		}))
	})
})
