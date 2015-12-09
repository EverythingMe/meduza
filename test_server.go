package meduza

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"
)

type TestServer struct {
	cmd     *exec.Cmd
	running bool
	port    int
	ctlPort int
}

const (
	// The env var for the meduza executable. If not set, we default to running "meduzad" from PATH
	MeduzaBinEnvvar = "MEDUZA_BIN"
	mdzCommand      = "meduzad"

	launchWaitTimeout = 250 * time.Millisecond
)

// Start and run the process, return an error if it cannot be run
func (s *TestServer) run() error {

	ret := s.cmd.Start()

	ch := make(chan error)

	// we wait for LaunchWaitTimeout and see if the server quit due to an error
	go func() {
		err := s.cmd.Wait()
		select {
		case ch <- err:
		default:
		}
	}()

	select {
	case e := <-ch:
		log.Println("Error waiting for process:", e)
		return e
	case <-time.After(launchWaitTimeout):
		break

	}

	return ret
}

// Create and run a new server on a given port.
// Return an error if the server cannot be started
func NewTestServer(port, ctlPort int, cmdlineArgs ...string) (*TestServer, error) {

	mdz := os.Getenv("MEDUZA_BIN")
	if mdz == "" {
		mdz = mdzCommand
	}

	cmdlineArgs = append(cmdlineArgs, "-test",
		fmt.Sprintf("-port=%d", port), fmt.Sprintf("-ctl_port=%d", ctlPort))

	cmd := exec.Command(mdz, cmdlineArgs...)

	log.Println("start args: ", cmd.Args)

	s := &TestServer{
		cmd:     cmd,
		running: false,
		port:    port,
		ctlPort: ctlPort,
	}

	err := s.run()
	if err != nil {
		return nil, err
	}
	s.running = true

	return s, nil

}

func (s *TestServer) Stop() error {
	if !s.running {
		return nil
	}
	s.running = false
	if err := s.cmd.Process.Kill(); err != nil {
		return err
	}

	s.cmd.Wait()

	return nil

}

func (s *TestServer) CtlUrl() string {
	return fmt.Sprintf("http://127.0.0.1:%d", s.ctlPort)
}
func (s *TestServer) DeploySchema(r io.Reader) error {

	u := fmt.Sprintf("%s/deploy", s.CtlUrl())

	res, err := http.Post(u, "text/yaml", r)

	if err != nil {
		return fmt.Errorf("Could not post deploy request to server: %s", err)
	}
	defer res.Body.Close()

	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Could not read response body: %s", err)
	}

	rc := string(b)
	if rc == "OK" {
		log.Println("Schema deployed successfully")
	} else {
		return fmt.Errorf("Error deploying schema. Server error: %s", s)
	}

	return nil

}
