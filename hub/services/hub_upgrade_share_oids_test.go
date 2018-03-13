package services_test

import (
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"

	"time"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"google.golang.org/grpc"
)

var _ = Describe("hub", func() {
	Describe("UpgradeShareOids", func() {
		var (
			reader                      configutils.Reader
			hub                         *services.HubClient
			fakeUpgradeShareOidsRequest *pb.UpgradeShareOidsRequest
			testStdout                  *gbytes.Buffer
		)
		BeforeEach(func() {
			reader = configutils.NewReader()
			hub, _ = services.NewHub(nil, &reader, grpc.DialContext)
			fakeUpgradeShareOidsRequest = &pb.UpgradeShareOidsRequest{}
			testStdout, _, _ = testhelper.SetupTestLogger()
			testutils.CleanUpDirectory("share-oids")
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
			hub.UpgradeShareOids(nil, fakeUpgradeShareOidsRequest)
			time.Sleep(3 * time.Second)
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

			reader := configutils.NewReader()
			hub, _ := services.NewHub(nil, &reader, grpc.DialContext)
			_, err := hub.UpgradeShareOids(nil, fakeUpgradeShareOidsRequest)
			Eventually(testStdout).Should(gbytes.Say("Started processing share-oids request"))
			Expect(err).To(BeNil())
		})
	})
})
