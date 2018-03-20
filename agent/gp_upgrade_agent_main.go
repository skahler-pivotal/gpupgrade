package main

import (
	"net"
	"os"
	"os/exec"

	"gp_upgrade/agent/services"
	"gp_upgrade/helpers"
	pb "gp_upgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":6416"
)

func main() {
	//debug.SetTraceback("all")
	//parser := flags.NewParser(&AllServices, flags.HelpFlag|flags.PrintErrors)
	//
	//_, err := parser.Parse()
	//if err != nil {
	//	os.Exit(utils.GetExitCodeForError(err))
	//}
	var logdir string
	var RootCmd = &cobra.Command{
		Use:   "gp_upgrade_agent ",
		Short: "Start the Command Listener (blocks)",
		Long:  `Start the Command Listener (blocks)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			gplog.InitializeLogging("gp_upgrade_agent", logdir)
			errorChannel := make(chan error)
			defer close(errorChannel)
			lis, err := net.Listen("tcp", port)
			if err != nil {
				gplog.Fatal(err, "failed to listen")
				return err
			}

			server := grpc.NewServer()

			commandExecer := func(command string, vars ...string) helpers.Command {
				return exec.Command(command, vars...)
			}
			agentServer := services.NewAgentServer(commandExecer)
			pb.RegisterAgentServer(server, agentServer)
			reflection.Register(server)

			go func(myListener net.Listener) {
				if err := server.Serve(myListener); err != nil {
					gplog.Fatal(err, "failed to serve", err)
					errorChannel <- err
				}

				close(errorChannel)
			}(lis)

			select {
			case err := <-errorChannel:
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	RootCmd.PersistentFlags().StringVar(&logdir, "log-directory", "", "command_listener log directory")

	if err := RootCmd.Execute(); err != nil {
		gplog.Error(err.Error())
		os.Exit(1)
	}
}
