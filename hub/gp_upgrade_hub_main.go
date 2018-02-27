package main

import (
	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/services"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "gp_upgrade/idl"

	"os"
	"runtime/debug"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
)

const (
	cliToHubPort = ":7527"
)

// This directory to have the implementation code for the gRPC server to serve
// Minimal CLI command parsing to embrace that booting this binary to run the hub might have some flags like a log dir

func main() {
	var logdir string
	var RootCmd = &cobra.Command{
		Use:   "gp_upgrade_hub [--log-directory path]",
		Short: "Start the gp_upgrade_hub (blocks)",
		Long:  `Start the gp_upgrade_hub (blocks)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			debug.SetTraceback("all")
			gplog.InitializeLogging("gp_upgrade_hub", logdir)
			lis, err := net.Listen("tcp", cliToHubPort)
			if err != nil {
				gplog.Fatal(err, "failed to listen")
			}

			server := grpc.NewServer()
			clusterPair := cluster.Pair{}
			myImpl := services.NewCliToHubListener(&clusterPair)
			pb.RegisterCliToHubServer(server, myImpl)
			reflection.Register(server)
			if err := server.Serve(lis); err != nil {
				gplog.Fatal(err, "failed to serve", err)
			}
			return nil
		},
	}

	RootCmd.PersistentFlags().StringVar(&logdir, "log-directory", "", "gp_upgrade_hub log directory")

	if err := RootCmd.Execute(); err != nil {
		gplog.Error(err.Error())
		os.Exit(1)
	}

}
