package services_test

import (
	"errors"
	"io/ioutil"
	"os"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"
	"gp_upgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	Describe("UpgradeShareOids", func() {
		var (
			reader        *spyReader
			hub           *services.HubClient
			dir           string
			commandExecer *testutils.FakeCommandExecer
			errChan       chan error
			outChan       chan []byte
		)

		BeforeEach(func() {
			reader = &spyReader{
				hostnames: []string{"hostone", "hosttwo"},
				segmentConfiguration: configutils.SegmentConfiguration{
					{
						Content:  0,
						DBID:     2,
						Hostname: "hostone",
					}, {
						Content:  1,
						DBID:     3,
						Hostname: "hosttwo",
					},
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
			testhelper.SetupTestLogger()
		})

		AfterEach(func() {
			os.RemoveAll(dir)
			utils.InitializeSystemFunctions()
		})

		It("Reports status PENDING when no share-oids request has been made", func() {
			stepStatus, err := testutils.GetUpgradeStatus(hub, pb.UpgradeSteps_SHARE_OIDS)
			Expect(err).To(BeNil())
			Eventually(stepStatus).Should(Equal(pb.StepStatus_PENDING))
		})

		It("marks step as FAILED if rsync fails for any host", func() {
			numHosts := len(reader.hostnames)

			errChan <- errors.New("failure")
			outChan <- nil

			errChan <- nil
			outChan <- []byte("success")

			hub.ShareOidFilesStub(dir)

			Eventually(commandExecer.GetNumInvocations()).Should(Equal(numHosts))

			stepStatus, err := testutils.GetUpgradeStatus(hub, pb.UpgradeSteps_SHARE_OIDS)
			Expect(err).To(BeNil())
			Eventually(stepStatus).Should(Equal(pb.StepStatus_FAILED))

		})

		It("marks step as COMPLETE if rsync succeeds for all hosts", func() {
			numHosts := len(reader.hostnames)

			errChan <- nil
			outChan <- []byte("success")

			errChan <- nil
			outChan <- []byte("success")

			hub.ShareOidFilesStub(dir)

			Eventually(commandExecer.GetNumInvocations()).Should(Equal(numHosts))

			stepStatus, err := testutils.GetUpgradeStatus(hub, pb.UpgradeSteps_SHARE_OIDS)
			Expect(err).To(BeNil())
			Eventually(stepStatus).Should(Equal(pb.StepStatus_COMPLETE))
		})
	})
})
