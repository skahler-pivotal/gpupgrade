package services_test

import (
	"gp_upgrade/hub/services"
	"gp_upgrade/testutils"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"gp_upgrade/hub/configutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HubClient", func() {
	var (
		reader *spyReader
		agentA *testutils.MockAgentServer
		port   int
	)

	BeforeEach(func() {
		reader = newSpyReader()
		agentA, port = testutils.NewMockAgentServer()
	})

	AfterEach(func() {
		agentA.Stop()
	})

	It("closes open connections when shutting down", func(done Done) {
		defer close(done)
		reader.hostnames = []string{"localhost"}
		hub := services.NewHub(nil, reader, grpc.DialContext, nil, &services.HubConfig{
			HubToAgentPort: port,
		})
		go hub.Start()

		By("creating connections")
		conns, err := hub.AgentConns()
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() connectivity.State { return conns[0].Conn.GetState() }).Should(Equal(connectivity.Ready))

		By("closing the connections")
		hub.Stop()
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() connectivity.State { return conns[0].Conn.GetState() }).Should(Equal(connectivity.Shutdown))
	})

	It("retrieves the agent connections from the config file reader", func() {
		reader.hostnames = []string{"localhost", "localhost"}
		hub := services.NewHub(nil, reader, grpc.DialContext, nil, &services.HubConfig{
			HubToAgentPort: port,
		})

		conns, err := hub.AgentConns()
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() connectivity.State { return conns[0].Conn.GetState() }).Should(Equal(connectivity.Ready))
		Expect(conns[0].Hostname).To(Equal("localhost"))
		Eventually(func() connectivity.State { return conns[1].Conn.GetState() }).Should(Equal(connectivity.Ready))
		Expect(conns[1].Hostname).To(Equal("localhost"))
	})

	It("saves grpc connections for future calls", func() {
		reader.hostnames = []string{"localhost"}

		hub := services.NewHub(nil, reader, grpc.DialContext, nil, &services.HubConfig{
			HubToAgentPort: port,
		})

		newConns, err := hub.AgentConns()
		Expect(err).ToNot(HaveOccurred())
		Expect(newConns).To(HaveLen(1))

		savedConns, err := hub.AgentConns()
		Expect(err).ToNot(HaveOccurred())
		Expect(savedConns).To(HaveLen(1))

		Expect(newConns[0]).To(Equal(savedConns[0]))
	})

	It("returns an error if any connections have non-ready states", func() {
		reader.hostnames = []string{"localhost"}
		hub := services.NewHub(nil, reader, grpc.DialContext, nil, &services.HubConfig{
			HubToAgentPort: port,
		})

		conns, err := hub.AgentConns()
		Expect(err).ToNot(HaveOccurred())
		Expect(conns).To(HaveLen(1))

		agentA.Stop()

		Eventually(func() connectivity.State { return conns[0].Conn.GetState() }).Should(Equal(connectivity.TransientFailure))

		_, err = hub.AgentConns()
		Expect(err).To(HaveOccurred())
	})

	It("returns an error if any connections have non-ready states when first dialing", func() {
		reader.hostnames = []string{"localhost"}
		hub := services.NewHub(nil, reader, grpc.DialContext, nil, &services.HubConfig{
			HubToAgentPort: port,
		})

		agentA.Stop()

		_, err := hub.AgentConns()
		Expect(err).To(HaveOccurred())
	})

	It("returns an error if the grpc dialer to the agent throws an error", func() {
		agentA.Stop()

		reader.hostnames = []string{"example"}
		hub := services.NewHub(nil, reader, grpc.DialContext, nil, &services.HubConfig{
			HubToAgentPort: port,
		})

		_, err := hub.AgentConns()
		Expect(err).To(HaveOccurred())
	})

	It("returns an error if the config reader fails", func() {
		reader.hostnamesErr = errors.New("error occurred while getting hostnames")
		hub := services.NewHub(nil, reader, nil, nil, &services.HubConfig{})

		_, err := hub.AgentConns()
		Expect(err).To(HaveOccurred())
	})
})

type spyReader struct {
	hostnames            []string
	hostnamesErr         error
	port                 chan int
	segmentConfiguration configutils.SegmentConfiguration
}

func newSpyReader() *spyReader {
	return &spyReader{}
}

func (r *spyReader) GetHostnames() ([]string, error) {
	return r.hostnames, r.hostnamesErr
}

func (r *spyReader) GetSegmentConfiguration() configutils.SegmentConfiguration {
	return r.segmentConfiguration
}

func (r *spyReader) OfOldClusterConfig(string) {}

func (r *spyReader) OfNewClusterConfig(string) {}

func (r *spyReader) GetPortForSegment(segmentDbid int) int {
	if len(r.port) == 0 {
		return -1
	}
	return <-r.port
}
