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

	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("UpgradeShareOids", func() {
	var (
		reader        *testutils.SpyReader
		hub           *services.HubClient
		dir           string
		commandExecer *testutils.FakeCommandExecer
		errChan       chan error
		outChan       chan []byte
	)

	BeforeEach(func() {
		segConfs := make(chan configutils.SegmentConfiguration, 2)
		reader = &testutils.SpyReader{
			Hostnames:             []string{"hostone", "hosttwo"},
			SegmentConfigurations: segConfs,
		}

		segConfs <- configutils.SegmentConfiguration{
			{
				Content:  0,
				DBID:     2,
				Hostname: "hostone",
			}, {
				Content:  1,
				DBID:     3,
				Hostname: "hosttwo",
			},
		}

		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		errChan = make(chan error, 2)
		outChan = make(chan []byte, 2)
		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{
			Err: errChan,
			Out: outChan,
		})

		hub = services.NewHub(nil, reader, grpc.DialContext, commandExecer.Exec, &services.HubConfig{
			StateDir: dir,
		})
	})

	AfterEach(func() {
		os.RemoveAll(dir)
	})

	It("Reports status PENDING when no share-oids request has been made", func() {
		stateChecker := upgradestatus.NewStateCheck(filepath.Join(dir, "share-oids"), pb.UpgradeSteps_SHARE_OIDS)
		Eventually(func() *pb.UpgradeStepStatus {
			status, _ := stateChecker.GetStatus()
			return status
		}).Should(Equal(&pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_SHARE_OIDS,
			Status: pb.StepStatus_PENDING,
		}))
	})

	It("marks step as COMPLETE if rsync succeeds for all hosts", func() {
		outChan <- []byte("success")
		outChan <- []byte("success")

		_, err := hub.UpgradeShareOids(nil, &pb.UpgradeShareOidsRequest{})
		Expect(err).ToNot(HaveOccurred())

		hostnames, err := reader.GetHostnames()
		Expect(err).ToNot(HaveOccurred())
		Eventually(commandExecer.GetNumInvocations).Should(Equal(len(hostnames)))

		stateChecker := upgradestatus.NewStateCheck(filepath.Join(dir, "share-oids"), pb.UpgradeSteps_SHARE_OIDS)
		Eventually(func() *pb.UpgradeStepStatus {
			status, _ := stateChecker.GetStatus()
			return status
		}).Should(Equal(&pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_SHARE_OIDS,
			Status: pb.StepStatus_COMPLETE,
		}))
	})

	It("marks step as FAILED if rsync fails for any host", func() {
		errChan <- errors.New("failure")
		outChan <- []byte("success")

		_, err := hub.UpgradeShareOids(nil, &pb.UpgradeShareOidsRequest{})
		Expect(err).ToNot(HaveOccurred())

		hostnames, err := reader.GetHostnames()
		Expect(err).ToNot(HaveOccurred())
		Eventually(commandExecer.GetNumInvocations).Should(Equal(len(hostnames)))

		stateChecker := upgradestatus.NewStateCheck(filepath.Join(dir, "share-oids"), pb.UpgradeSteps_SHARE_OIDS)
		Eventually(func() *pb.UpgradeStepStatus {
			status, _ := stateChecker.GetStatus()
			return status
		}).Should(Equal(&pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_SHARE_OIDS,
			Status: pb.StepStatus_FAILED,
		}))
	})
})
