package jobmanager

import (
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	io_prometheus_client "github.com/prometheus/client_model/go"
	//prefmsg "github.com/davidbirdsong/go-prefixbytes"
)

type cmdTester struct {
	path string
	args []string
	dir  string
}

func (c *cmdTester) Run(i uint64) *exec.Cmd {
	return &exec.Cmd{
		Path: c.path,
		Args: c.args,
		Dir:  c.dir,
	}

}

func counterToFloat(counter *prometheus.CounterVec, l prometheus.Labels) (float64, error) {
	var metric = &io_prometheus_client.Metric{}
	c := counter.With(l)

	if err := c.Write(metric); err != nil {
		return 0, err
	}
	return *metric.Counter.Value, nil

}

var bookeepIntervalMs = time.Millisecond * 500

func basicSetup(t *testing.T, bin string, args []string,
	min, max int64, maxrss uint64) *Jobmanager {

	if !filepath.IsAbs(bin) {
		var err error
		if bin, err = exec.LookPath("levee"); err != nil {
			t.Fatal(err)
		}
	}

	jb, err := NewJobManager(
		&cmdTester{
			path: bin,
			args: args,
			dir:  "testdata",
		},
		"",
		"",
		t.Name(),
		min, max, // 1 min proc, 10 maxprocs
		maxrss, //52 MB
	)
	if err != nil {
		t.Fatal(err)
		return jb

	}

	jb.Run(bookeepIntervalMs)
	return jb
}

func leveeSetup(t *testing.T, script string, args []string,
	min, max int64, maxrss uint64) *Jobmanager {
	if levee, err := exec.LookPath("levee"); err != nil {
		t.Fatal(err)
	} else {
		newArgs := []string{levee, "run", script}
		for _, a := range args {
			newArgs = append(newArgs, a)
		}
		return basicSetup(t, levee, newArgs, min, max, maxrss)
	}
	t.Fatalf("[BUG] no Jobmanager")
	return nil

}
func TestEofJob(t *testing.T) {
	min, max := int64(2), int64(10)
	maxrss := uint64(1024 * 1024 * 10)
	jb := leveeSetup(t, "simplespin.lua", []string{},
		min, max, maxrss)

	j := jb.Reserve()
	syscall.Kill(j.cmd.Process.Pid, syscall.SIGKILL)

	j.Communicate([]byte("DKFJDKJF"))
	jb.Release(j)
	jb.Stop()

	if f, err := counterToFloat(jb.jobactions,
		prometheus.Labels{"action": "died"}); err != nil {
		t.Fatal(err)
	} else if f != 1 {
		t.Errorf("expected 1 jobs post-init, got: %f\n", f)
	}

}

func TestMinMaxJob(t *testing.T) {
	min, max := int64(2), int64(10)
	maxrss := uint64(1024 * 1024 * 10)
	jb := leveeSetup(t, "simplespin.lua", []string{},
		min, max, maxrss)

	jlist := make([]*job, 0)

	if f, err := counterToFloat(jb.jobactions,
		prometheus.Labels{"action": "new_job"}); err != nil {
		t.Fatal(err)
	} else if f != 2 {
		t.Errorf("expected 2 jobs post-init, got: %f\n", f)
	}

	for i := max; i > 0; i-- {
		jlist = append(jlist, jb.Reserve())
	}

	j := jlist[len(jlist)-1]
	jlist = jlist[:len(jlist)-1]
	go func(jh *job) {
		time.Sleep(time.Millisecond * 10)
		jb.Release(jh)

	}(j)
	j = jb.Reserve()
	jb.Release(j)

	if f, err := counterToFloat(jb.jobactions,
		prometheus.Labels{"action": "wait_maxjobs"}); err != nil {
		t.Fatal(err)
	} else if f != 2 {
		t.Errorf("expected 1 wait_maxjobs, got: %f\n", f)
	}

	for _, j := range jlist {
		jb.Release(j)
	}

}

func TestKillSlowJob(t *testing.T) {
	bin, err := exec.LookPath("levee")
	if err != nil {
		t.Fatal(err)
	}
	bookeepinterval_ms := time.Duration(500)

	jb, err := NewJobManager(
		&cmdTester{
			path: bin,
			args: []string{
				bin,
				"run",
				"slow.lua",
				fmt.Sprintf("%d", bookeepinterval_ms),
			},
			dir: "testdata",
		},
		"test",
		"",
		"levee_test",
		1, 1, // 1 min proc, 10 maxprocs
		1024*1024*600, //52 MB
	)
	if err != nil {
		t.Fatal(err)
	}

	jb.Run(time.Millisecond * bookeepinterval_ms)
	j := jb.Reserve()
	if j == nil {
		t.Fatalf("job manager returned nil job")
	}

	if _, _, err := j.Communicate([]byte("comms!!!!\n")); err != io.EOF {
		t.Fatalf("job should have timed out, caused error: %v", err)
	}
	time.Sleep(3 * time.Second)
	if f, err := counterToFloat(jb.jobactions,
		prometheus.Labels{"action": "job_timeout"}); err != nil {
		t.Fatal(err)
	} else if f < 1 {
		t.Errorf("expected timeout job counter ticked up")
	}
}

func TestRSSKill(t *testing.T) {
	bin, err := exec.LookPath("levee")
	if err != nil {
		t.Fatal(err)
	}
	jb, err := NewJobManager(
		&cmdTester{
			path: bin,
			args: []string{
				bin,
				"run",
				"memhog.lua",
			},
			dir: "testdata",
		},
		"test",
		"",
		"levee_test",
		30, 40, // 1 min proc, 10 maxprocs
		1024*1024*6, //52 MB
	)
	if err != nil {
		t.Fatal(err)
	}
	if jb == nil {
		t.Fatal("no job manager")
	}
	for _, m := range jb.Metrics() {
		prometheus.MustRegister(m)

	}
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":8080", nil)

	jb.Run(time.Second * 3)

	workC := make(chan int)
	wg := &sync.WaitGroup{}
	for i := 1; i <= 30; i++ {
		wg.Add(1)
		go func() {
			for range workC {
				//fmt.Printf("[TEST] loop count: %d\n", i)
				j := jb.Reserve()
				if j == nil {
				}

				//t := time.Now()
				for k := 1; k <= 3; k++ {
					_, f, err := j.Communicate([]byte("comms!!!!\n"))
					if err != nil {
						if k == 3 {
							t.Fatal(err)
						}
						continue
					}

					jb.Release(j)
					f()
					break
				}

			}
			wg.Done()
		}()
	}
	for i := 1; i <= 100000; i++ {
		workC <- i

	}
	close(workC)
	wg.Wait()

	jb.Stop()
}
