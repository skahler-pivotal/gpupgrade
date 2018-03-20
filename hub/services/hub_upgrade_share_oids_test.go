package services_test

import (
	"io/ioutil"
	"os"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	Describe("UpgradeShareOids", func() {
		var (
			reader     configutils.Reader
			hub        *services.HubClient
			testStdout *gbytes.Buffer
			dir        string
		)

		BeforeEach(func() {
			reader = configutils.NewReader()
			var err error
			dir, err = ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())
			hub = services.NewHub(nil, &reader, grpc.DialContext, nil, &services.HubConfig{
				StateDir: dir,
			})

			testStdout, _, _ = testhelper.SetupTestLogger()
		})

		AfterEach(func() {
			os.RemoveAll(dir)
		})

		It("Reports status PENDING when no share-oids request has been made", func() {
			reply, err := hub.StatusUpgrade(nil, &pb.StatusUpgradeRequest{})
			Expect(err).To(BeNil())
			stepStatuses := reply.GetListOfUpgradeStepStatuses()

			var stepStatusSaved *pb.UpgradeStepStatus
			for _, stepStatus := range stepStatuses {
				if stepStatus.GetStep() == pb.UpgradeSteps_SHARE_OIDS {
					stepStatusSaved = stepStatus
				}
			}

			Expect(stepStatusSaved.GetStep()).ToNot(BeZero())
			Expect(stepStatusSaved.GetStatus()).To(Equal(pb.StepStatus_PENDING))
		})

		It("Reports status COMPLETED when a share-oids request has been made", func() {
			hub.UpgradeShareOids(nil, &pb.UpgradeShareOidsRequest{})
			reply, err := hub.StatusUpgrade(nil, &pb.StatusUpgradeRequest{})

			Expect(err).To(BeNil())
			stepStatuses := reply.GetListOfUpgradeStepStatuses()

			var stepStatusSaved *pb.UpgradeStepStatus
			for _, stepStatus := range stepStatuses {
				if stepStatus.GetStep() == pb.UpgradeSteps_SHARE_OIDS {
					stepStatusSaved = stepStatus
				}
			}

			Expect(stepStatusSaved.GetStep()).ToNot(BeZero())
			Eventually(stepStatusSaved.GetStatus()).Should(Equal(pb.StepStatus_COMPLETE))
		})

		It("Starts sharing OID files across cluster without error", func() {
			_, err := hub.UpgradeShareOids(nil, &pb.UpgradeShareOidsRequest{})
			Eventually(testStdout).Should(gbytes.Say("Started processing share-oids request"))
			Expect(err).To(BeNil())
		})
	})
})
