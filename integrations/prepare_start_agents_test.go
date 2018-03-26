package integrations_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/hub/services"
	"gp_upgrade/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"google.golang.org/grpc"
)

var _ = Describe("prepare", func() {
	var (
		dir            string
		hub            *services.HubClient
		commandExecer  *testutils.FakeCommandExecer
		cliToHubPort   int
		hubToAgentPort int
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		cliToHubPort = 7527
		hubToAgentPort = 6416

		conf := &services.HubConfig{
			CliToHubPort:   cliToHubPort,
			HubToAgentPort: hubToAgentPort,
			StateDir:       dir,
		}
		reader := configutils.NewReader()

		commandExecer = &testutils.FakeCommandExecer{}
		commandExecer.SetOutput(&testutils.FakeCommand{})

		hub = services.NewHub(&cluster.Pair{}, &reader, grpc.DialContext, commandExecer.Exec, conf)

		port := os.Getenv("PGPORT")

		clusterConfig := fmt.Sprintf(`[{
              "content": -1,
              "dbid": 1,
              "hostname": "localhost",
              "datadir": "%s",
              "mode": "s",
              "preferred_role": "m",
              "role": "m",
              "status": "u",
              "port": %s
        }]`, dir, port)

		testutils.WriteProvidedConfig(dir, clusterConfig)

		Expect(checkPortIsAvailable(conf.CliToHubPort)).To(BeTrue())
		go hub.Start()
	})

	AfterEach(func() {
		hub.Stop()
		Expect(checkPortIsAvailable(cliToHubPort)).To(BeTrue())
		os.RemoveAll(dir)
	})

	Describe("start-agents", func() {
		It("updates status PENDING to RUNNING then to COMPLETE if successful", func(done Done) {
			defer close(done)
			Expect(runStatusUpgrade()).To(ContainSubstring("PENDING - Agents Started on Cluster"))

			trigger := make(chan struct{}, 1)
			commandExecer.SetTrigger(trigger)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()

				Eventually(runStatusUpgrade).Should(ContainSubstring("RUNNING - Agents Started on Cluster"))
				trigger <- struct{}{}
			}()

			prepareStartAgentsSession := runCommand("prepare", "start-agents")
			Eventually(prepareStartAgentsSession).Should(Exit(0))
			wg.Wait()

			Expect(commandExecer.Command()).To(Equal("ssh"))
			Expect(strings.Join(commandExecer.Args(), "")).To(ContainSubstring("nohup"))
			Eventually(runStatusUpgrade).Should(ContainSubstring("COMPLETE - Agents Started on Cluster"))
		})
	})
})
