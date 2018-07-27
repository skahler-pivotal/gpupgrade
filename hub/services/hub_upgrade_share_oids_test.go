package services_test

import (
	"errors"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	pb "github.com/greenplum-db/gpupgrade/idl"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("UpgradeShareOids", func() {
	var (
		testExecutor *testhelper.TestExecutor
	)

	BeforeEach(func() {
		testExecutor = &testhelper.TestExecutor{}
		source.Executor = testExecutor
	})

	It("copies files to each host", func() {
		seg1 := source.Segments[1]
		seg1.Hostname = "not_localhost"
		source.Segments[1] = seg1
		_, err := hub.UpgradeShareOids(nil, &pb.UpgradeShareOidsRequest{})
		Expect(err).ToNot(HaveOccurred())

		hostnames := source.GetHostnames()
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() int { return testExecutor.NumExecutions }).Should(Equal(len(hostnames)))

		Expect(testExecutor.LocalCommands).To(ConsistOf([]string{
			fmt.Sprintf("rsync -rzpogt %s/pg_upgrade/pg_upgrade_dump_*_oids.sql gpadmin@localhost:%s/pg_upgrade", dir, dir),
			fmt.Sprintf("rsync -rzpogt %s/pg_upgrade/pg_upgrade_dump_*_oids.sql gpadmin@not_localhost:%s/pg_upgrade", dir, dir),
		}))
	})

	It("copies all files even if rsync fails for a host", func() {
		testExecutor.LocalError = errors.New("failure")

		_, err := hub.UpgradeShareOids(nil, &pb.UpgradeShareOidsRequest{})
		Expect(err).ToNot(HaveOccurred())

		hostnames := source.GetHostnames()
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() int { return testExecutor.NumExecutions }).Should(Equal(len(hostnames)))
	})
})
