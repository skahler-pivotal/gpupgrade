package sshclient

type SSHClient interface {
	NewSession() (SSHSession, error)
}
