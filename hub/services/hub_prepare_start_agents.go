package services

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

// grpc generated function signature requires ctx and in params.
// nolint: unparam
func (h *Hub) PrepareStartAgents(ctx context.Context, in *idl.PrepareStartAgentsRequest) (*idl.PrepareStartAgentsReply, error) {
	gplog.Info("Running PrepareStartAgents()")

	step := h.checklist.GetStepWriter(upgradestatus.START_AGENTS)

	err := step.ResetStateDir()
	if err != nil {
		gplog.Error(err.Error())
		return &idl.PrepareStartAgentsReply{}, err
	}

	err = step.MarkInProgress()
	if err != nil {
		gplog.Error(err.Error())
		return &idl.PrepareStartAgentsReply{}, err
	}

	go StartAgents(h.source, step)

	return &idl.PrepareStartAgentsReply{}, nil
}

func StartAgents(source *utils.Cluster, step upgradestatus.StateWriter) {
	var err error

	// TODO: if this finds nothing, should we err out? do a fallback check based on $GPHOME?
	logStr := "start agents on master and hosts"
	agentPath := filepath.Join(source.BinDir, "gpupgrade_agent")
	runAgentCmd := func(contentID int) string { return agentPath + " --daemonize" }
	remoteOutput := source.GenerateAndExecuteCommand(logStr, runAgentCmd, cluster.ON_HOSTS_AND_MASTER)

	errStr := "Failed to start all gpupgrade_agents"
	errMessage := func(contentID int) string {
		return fmt.Sprintf("Could not start gpupgrade_agent on segment with contentID %d", contentID)
	}
	source.CheckClusterError(remoteOutput, errStr, errMessage, true)

	if remoteOutput.NumErrors > 0 {
		err = step.MarkFailed()
		if err != nil {
			gplog.Error(err.Error())
			return
		}
	}

	err = step.MarkComplete()
	if err != nil {
		gplog.Error(err.Error())
		return
	}
}
