package services_test

import (
	"errors"
	"io/ioutil"

	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/testutils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	var (
		dir           string
		commandExecer *testutils.FakeCommandExecer
		reader        *testutils.SpyReader
		hub           *services.HubClient
		mockAgent     *testutils.MockAgentServer
		segmentConfs  chan configutils.SegmentConfiguration
		port          int
	)

	BeforeEach(func() {
		testhelper.SetupTestLogger()

		segmentConfs = make(chan configutils.SegmentConfiguration, 2)
		reader = &testutils.SpyReader{
			Hostnames:             []string{"localhost", "localhost"},
			SegmentConfigurations: segmentConfs,
		}

		mockAgent, port = testutils.NewMockAgentServer()

		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		conf := &services.HubConfig{
			StateDir:       dir,
			HubToAgentPort: port,
		}

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})

		hub = services.NewHub(nil, reader, grpc.DialContext, commandExecer.Exec, conf)
	})
	AfterEach(func() {
		defer mockAgent.Stop()
	})

	It("returns no error when upgrade primary calls to the agents work", func() {
		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "old/datadir1", 1),
			newSegment(1, "localhost", "p", "old/datadir2", 2),
			newSegment(1, "localhost", "m", "old/datadir2/mirror", 3),
			newSegment(-1, "localhost", "p", "old/master", 4),
		}

		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "new/datadir1", 11),
			newSegment(1, "localhost", "p", "new/datadir2", 22),
			newSegment(1, "localhost", "m", "new/datadir2/mirror", 33),
			newSegment(-1, "localhost", "p", "new/master", 44),
		}

		_, err := hub.UpgradeConvertPrimaries(nil, &pb.UpgradeConvertPrimariesRequest{
			OldBinDir: "/old/bin",
			NewBinDir: "/new/bin",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(mockAgent.UpgradeConvertPrimarySegmentsRequest.OldBinDir).To(Equal("/old/bin"))
		Expect(mockAgent.UpgradeConvertPrimarySegmentsRequest.NewBinDir).To(Equal("/new/bin"))
		Expect(mockAgent.UpgradeConvertPrimarySegmentsRequest.DataDirPairs).To(ConsistOf([]*pb.DataDirPair{
			{OldDataDir: "old/datadir1", NewDataDir: "new/datadir1", Content: 0, OldPort: 1, NewPort: 11},
			{OldDataDir: "old/datadir2", NewDataDir: "new/datadir2", Content: 1, OldPort: 2, NewPort: 22},
		}))
	})

	It("returns an error if new config does not contain all the same content as the old config", func() {
		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "old/datadir1", 1),
			newSegment(1, "localhost", "p", "old/datadir2", 2),
		}

		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "new/datadir1", 11),
		}

		_, err := hub.UpgradeConvertPrimaries(nil, &pb.UpgradeConvertPrimariesRequest{
			OldBinDir: "/old/bin",
			NewBinDir: "/new/bin",
		})
		Expect(err).To(HaveOccurred())
		Expect(mockAgent.NumberOfCalls()).To(Equal(0))
	})

	It("returns an error if the content matches, but the hostname does not", func() {
		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "old/datadir1", 1),
			newSegment(1, "localhost", "p", "old/datadir2", 2),
		}

		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "new/datadir1", 11),
			newSegment(1, "localhost2", "p", "old/datadir2", 22),
		}

		_, err := hub.UpgradeConvertPrimaries(nil, &pb.UpgradeConvertPrimariesRequest{
			OldBinDir: "/old/bin",
			NewBinDir: "/new/bin",
		})
		Expect(err).To(HaveOccurred())

		Expect(mockAgent.NumberOfCalls()).To(Equal(0))
	})

	It("returns an error if any upgrade primary call to any agent fails", func() {
		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "old/datadir1", 1),
			newSegment(1, "localhost", "p", "old/datadir2", 2),
		}

		segmentConfs <- configutils.SegmentConfiguration{
			newSegment(0, "localhost", "p", "new/datadir1", 11),
			newSegment(1, "localhost", "p", "new/datadir2", 22),
		}

		mockAgent.Err <- errors.New("fail upgrade primary call")

		_, err := hub.UpgradeConvertPrimaries(nil, &pb.UpgradeConvertPrimariesRequest{
			OldBinDir: "/old/bin",
			NewBinDir: "/new/bin",
		})
		Expect(err).To(HaveOccurred())

		Expect(mockAgent.NumberOfCalls()).To(Equal(2))
	})

	It("returns an error if the agent is inaccessible", func() {
		mockAgent.Stop()

		_, err := hub.UpgradeConvertPrimaries(nil, &pb.UpgradeConvertPrimariesRequest{
			OldBinDir: "/old/bin",
			NewBinDir: "/new/bin",
		})
		Expect(err).To(HaveOccurred())

		Expect(mockAgent.NumberOfCalls()).To(Equal(0))
	})
})

func newSegment(content int, hostname, preferredRole, dataDir string, port int) configutils.Segment {
	return configutils.Segment{
		Content:       content,
		Hostname:      hostname,
		PreferredRole: preferredRole,
		Datadir:       dataDir,
		Port:          port,
	}
}
