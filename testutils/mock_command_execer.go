package testutils

import (
	"sync"

	"gp_upgrade/helpers"
)

type FakeCommandExecer struct {
	command string
	args    []string

	mu      sync.Mutex
	output  helpers.Command
	trigger chan struct{}
}

func (c *FakeCommandExecer) SetOutput(command helpers.Command) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.output = command
}

func (c *FakeCommandExecer) SetTrigger(trigger chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.trigger = trigger
}

func (c *FakeCommandExecer) Command() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.command
}

func (c *FakeCommandExecer) Args() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.args
}

func (c *FakeCommandExecer) Exec(command string, args ...string) helpers.Command {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.command = command
	c.args = args

	if c.trigger != nil {
		<-c.trigger
	}

	return c.output
}

type FakeCommand struct {
	Err error
	Out []byte
}

func (c *FakeCommand) Output() ([]byte, error) {
	return c.Out, c.Err
}

func (c *FakeCommand) CombinedOutput() ([]byte, error) {
	return c.Out, c.Err
}

func (c *FakeCommand) Start() error {
	return c.Err
}

func (c *FakeCommand) Run() error {
	return c.Err
}
