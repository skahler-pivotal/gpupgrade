package configutils

import (
	pb "gp_upgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"google.golang.org/grpc"
)

const (
	// todo generalize to any host
	// todo de-duplicate the use of this port in monitor.go
	port = "6416"
)

type ClientAndHostname struct {
	Client   pb.CommandListenerClient
	Hostname string
}

func GetRPCClients() ([]ClientAndHostname, error) {
	reader := NewReader()
	reader.OfOldClusterConfig()
	hostnames, err := reader.GetHostnames()
	if err != nil {
		return nil, err
	}

	var clients []ClientAndHostname
	for i := 0; i < len(hostnames); i++ {
		conn, err := grpc.Dial(hostnames[i]+":"+port, grpc.WithInsecure())
		if err != nil {
			gplog.Error(err.Error())
		}
		clientAndHost := ClientAndHostname{
			Client:   pb.NewCommandListenerClient(conn),
			Hostname: hostnames[i],
		}
		clients = append(clients, clientAndHost)
	}
	return clients, err
}
