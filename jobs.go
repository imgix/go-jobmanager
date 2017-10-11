package jobmanager

import (
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	prefmsg "github.com/davidbirdsong/go-prefixbytes"
	msgio "github.com/jbenet/go-msgio"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/process"
)

type job struct {
	useCount, totCount int64
	rss                uint64
	id                 int32
	start              time.Time
	cmd                *exec.Cmd
	proc               *process.Process
	stdin              io.Writer
	stderr             io.ReadCloser
	stdout             msgio.Reader
	running            bool
	runtime            *prometheus.HistogramVec
}

func (j *job) age() time.Duration {
	return time.Now().Sub(j.start)
}

func (j *job) stop(wait bool) {
	j.cmd.Process.Kill()
	if wait {

		go func() {
			j.cmd.Process.Wait()
		}()
	}
}
func (j *job) Communicate(b []byte) ([]byte, func(), error) {
	//return j.communicate(b)
	timer := prometheus.NewTimer(
		prometheus.ObserverFunc(func(v float64) {
			j.runtime.WithLabelValues("call").Observe(v)
		},
		),
	)
	defer timer.ObserveDuration()
	return j.communicate(b)

}
func (j *job) communicate(b []byte) ([]byte, func(), error) {

	// handle case where empty writes are desired
	if b != nil && len(b) > 0 {
		if _, err := j.stdin.Write(b); err != nil {
			switch err {
			case io.EOF, io.ErrUnexpectedEOF, syscall.EPIPE:
				j.running = false
			}

			return nil, nil, err
		}
	}
	if bstream, err := j.stdout.ReadMsg(); err != nil {
		switch err {
		case io.EOF, io.ErrUnexpectedEOF, syscall.EPIPE:
			j.running = false
		}
		return nil, nil, err
	} else {

		return bstream, func() { j.stdout.ReleaseMsg(bstream) }, nil
	}
	return nil, nil, nil
}

func newjob(jobid uint64, run Runner, v *prometheus.HistogramVec) (*job, error) {
	j := &job{
		cmd:     run.Run(jobid),
		id:      0,
		runtime: v,
	}
	j.cmd.Stderr = os.Stderr

	//j.stderr, _ = j.cmd.StderrPipe()
	stdout, _ := j.cmd.StdoutPipe()
	j.stdin, _ = j.cmd.StdinPipe()

	j.stdout = prefmsg.NewFixedintReader(stdout, 16)

	if err := j.cmd.Start(); err != nil {
		return nil, err
	}
	j.running = true

	j.id = int32(j.cmd.Process.Pid)
	if jinit, ok := run.(Initializer); ok {
		if err := jinit.Init(jobid, j); err != nil {
			j.stop(true)
			j.running = false
			return nil, err
		}
	}
	return j, nil
}
