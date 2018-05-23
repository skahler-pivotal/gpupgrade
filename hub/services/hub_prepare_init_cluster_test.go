package services_test

import (
	"errors"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"

	"github.com/greenplum-db/gpupgrade/hub/configutils"
	"github.com/greenplum-db/gpupgrade/hub/services"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/mock_idl"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("Hub prepare init-cluster", func() {
	var (
		dbConnector *dbconn.DBConn
		mock        sqlmock.Sqlmock
		dir         string
		err         error
		newBinDir   string
		queryResult = `{"SegConfig":[{"address":"mdw","content":-1,"datadir":"/data/master/gpseg-1","dbid":1,"hostname":"mdw","mode":"s","status":"u","port":15432,"preferred_role":"p","role":"p"},` +
			`{"address":"sdw1","content":0,"datadir":"/data/primary/gpseg-0","dbid":2,"hostname":"sdw1","mode":"s","status":"u","port":25432,"preferred_role":"p","role":"p"}],"BinDir":"/tmp"}`
	)

	BeforeEach(func() {
		newBinDir = "/tmp"
		dbConnector, mock = testhelper.CreateAndConnectMockDB(1)
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		operating.System = operating.InitializeSystemFunctions()
	})

	Describe("SaveTargetClusterConfig", func() {
		It("successfully stores target cluster config for GPDB 6", func() {
			testhelper.SetDBVersion(dbConnector, "6.0.0")

			congfigQuery := services.CONFIGQUERY6
			mock.ExpectQuery(congfigQuery).WillReturnRows(getFakeConfigRows())

			fakeConfigAndVersionFile := gbytes.NewBuffer()
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return fakeConfigAndVersionFile, nil
			}

			err = services.SaveTargetClusterConfig(dbConnector, dir, newBinDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(fakeConfigAndVersionFile.Contents())).To(ContainSubstring(queryResult))
		})

		It("successfully stores target cluster config for GPDB 4 and 5", func() {
			mock.ExpectQuery(services.CONFIGQUERY5).WillReturnRows(getFakeConfigRows())

			fakeConfigAndVersionFile := gbytes.NewBuffer()
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return fakeConfigAndVersionFile, nil
			}

			err = services.SaveTargetClusterConfig(dbConnector, dir, newBinDir)
			Expect(err).ToNot(HaveOccurred())

			Expect(string(fakeConfigAndVersionFile.Contents())).To(ContainSubstring(queryResult))
		})

		It("fails to get config file handle", func() {
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return nil, errors.New("failed to write config file")
			}

			err := services.SaveTargetClusterConfig(dbConnector, dir, newBinDir)
			Expect(err).To(HaveOccurred())
		})

		It("db.Select query for cluster config fails", func() {
			configQuery := services.CONFIGQUERY5
			mock.ExpectQuery(configQuery).WillReturnError(errors.New("fail config query"))

			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return gbytes.NewBuffer(), nil
			}

			err := services.SaveTargetClusterConfig(dbConnector, dir, newBinDir)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("Unable to execute query " + configQuery + ". Err: fail config query"))
		})
	})

	Describe("CheckAllFreePorts", func() {
		It("successfully finds a free port range", func() {
			ctrl := gomock.NewController(T)
			mockAgent1 := mock_idl.NewMockAgentClient(ctrl)
			mockAgent2 := mock_idl.NewMockAgentClient(ctrl)
			mockAgentConns := []*services.Connection{
				{PbAgentClient: mockAgent1},
				{PbAgentClient: mockAgent2},
			}
			defer ctrl.Finish()

			possiblePortBase := 1
			numPrimaries := 1

			mockAgent1.EXPECT().CheckFreePorts(gomock.Any(), &pb.CheckFreePortsRequest{
				PossiblePortBase: int32(possiblePortBase),
				NumPrimaries:     int32(numPrimaries),
			}).Return(&pb.CheckFreePortsReply{Result: true}, nil)

			mockAgent2.EXPECT().CheckFreePorts(gomock.Any(), &pb.CheckFreePortsRequest{
				PossiblePortBase: int32(possiblePortBase),
				NumPrimaries:     int32(numPrimaries),
			}).Return(&pb.CheckFreePortsReply{Result: true}, nil)

			result := services.CheckAllFreePorts(mockAgentConns, possiblePortBase, numPrimaries)
			Expect(result).To(BeTrue())
		})

		It("fails to find a free port range", func() {
			ctrl := gomock.NewController(T)
			mockAgent1 := mock_idl.NewMockAgentClient(ctrl)
			mockAgent2 := mock_idl.NewMockAgentClient(ctrl)
			mockAgentConns := []*services.Connection{
				{PbAgentClient: mockAgent1},
				{PbAgentClient: mockAgent2},
			}
			defer ctrl.Finish()

			possiblePortBase := 1
			numPrimaries := 1

			mockAgent1.EXPECT().CheckFreePorts(gomock.Any(), &pb.CheckFreePortsRequest{
				PossiblePortBase: int32(possiblePortBase),
				NumPrimaries:     int32(numPrimaries),
			}).Return(&pb.CheckFreePortsReply{Result: false}, errors.New("no free ports"))

			mockAgent2.EXPECT().CheckFreePorts(gomock.Any(), &pb.CheckFreePortsRequest{
				PossiblePortBase: int32(possiblePortBase),
				NumPrimaries:     int32(numPrimaries),
			}).Return(&pb.CheckFreePortsReply{Result: true}, nil)

			result := services.CheckAllFreePorts(mockAgentConns, possiblePortBase, numPrimaries)
			Expect(result).To(BeFalse())
		})
	})

	Describe("GetFreePortBase", func() {
		It("determines GetMaxSegmentPort+1 is a free port base", func() {
			mockReader := &testutils.SpyReader{}

			mockCheckAllFreePorts := func([]*services.Connection, int, int) bool {
				return true
			}

			mockAgentConns := []*services.Connection{}

			port, err := services.GetFreePortBase(mockReader, mockCheckAllFreePorts, mockAgentConns)
			Expect(port).To(Equal(mockReader.MaxPort + 1))
			Expect(err).ToNot(HaveOccurred())
		})

		It("determines GetMaxSegmentPort+1 is not a free port base, then finds a free port range", func() {
			mockReader := &testutils.SpyReader{}
			segConf := make(chan configutils.SegmentConfiguration, 2)
			mockReader.SegmentConfigurations = segConf
			segConf <- configutils.SegmentConfiguration{{}, {}}

			result := make(chan bool, 2)
			result <- false
			result <- true
			mockCheckAllFreePorts := func([]*services.Connection, int, int) bool {
				return <-result
			}

			mockAgentConns := []*services.Connection{}

			port, err := services.GetFreePortBase(mockReader, mockCheckAllFreePorts, mockAgentConns)
			numPrimaries := len(mockReader.GetSegmentConfiguration()) - 1
			Expect(port).To(SatisfyAll(
				BeNumerically(">", 1023),
				BeNumerically("<", math.MaxInt32-numPrimaries)),
			)
			Expect(err).ToNot(HaveOccurred())
		})

		It("does not find a free port", func() {
			mockReader := &testutils.SpyReader{}

			mockCheckAllFreePorts := func([]*services.Connection, int, int) bool {
				return false
			}

			mockAgentConns := []*services.Connection{}

			_, err := services.GetFreePortBase(mockReader, mockCheckAllFreePorts, mockAgentConns)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("GetNewMasterPort", func() {
		It("determines master+1 is a free port", func() {
			errChan := make(chan error, 2)
			outChan := make(chan []byte, 2)
			commandExecer := &testutils.FakeCommandExecer{}
			commandExecer.SetOutput(&testutils.FakeCommand{
				Err: errChan,
				Out: outChan,
			})
			outChan <- []byte("")
			errChan <- nil
			output, err := services.GetNewMasterPort(commandExecer.Exec, 15432)
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal(15433))
		})

		It("determines master+1 is not a free port, then finds a free port", func() {
			errChan := make(chan error, 2)
			outChan := make(chan []byte, 2)
			commandExecer := &testutils.FakeCommandExecer{}
			commandExecer.SetOutput(&testutils.FakeCommand{
				Err: errChan,
				Out: outChan,
			})
			outChan <- []byte("15433")
			errChan <- errors.New("exit status 1")
			utils.System.ResolveTCPAddr = func(network, address string) (*net.TCPAddr, error) {
				return &net.TCPAddr{Port: 42000}, nil
			}
			defer utils.InitializeSystemFunctions()

			output, err := services.GetNewMasterPort(commandExecer.Exec, 15432)
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal(42000))
		})

	})
})
