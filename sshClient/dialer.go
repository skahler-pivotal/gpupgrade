package sshclient

import "golang.org/x/crypto/ssh"

type Dialer interface {
	Dial(network, addr string, config *ssh.ClientConfig) (SSHClient, error)
}
