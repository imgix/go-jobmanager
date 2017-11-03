package jobmanager

import (
	"net/http"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jolestar/go-commons-pool"
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
	min, max int, maxrss uint64) *Jobmanager {

	if !filepath.IsAbs(bin) {
		var err error
		if bin, err = exec.LookPath("levee"); err != nil {
			t.Fatal(err)
		}
	}

	return NewJobmanager(
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
}

func leveeSetup(t *testing.T, script string, args []string,
	min, max int, maxrss uint64) *Jobmanager {
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
	min, max := 2, 10
	maxrss := uint64(1024 * 1024 * 10)
	jb := leveeSetup(t, "simplespin.lua", []string{},
		min, max, maxrss)

	j, err := jb.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	syscall.Kill(j.cmd.Process.Pid, syscall.SIGKILL)

	j.Communicate([]byte("DKFJDKJF"))
	jb.Release(j)
	jb.Stop()

}

func TestMinMaxJob(t *testing.T) {
	min, max := 2, 10
	maxrss := uint64(1024 * 1024 * 10)
	jb := leveeSetup(t, "simplespin.lua", []string{},
		min, max, maxrss)

	jlist := make([]*job, 0)

	for i := max + 1; i > 0; i-- {
		if j, e := jb.Reserve(); e != nil {
			if i == 1 {
				if err, ok := e.(*pool.NoSuchElementErr); !ok {
					t.Logf("didn't catch error: %#v", err)
				}
			} else {
				t.Fatalf("%d %#v", i, e)
			}
		} else {
			jlist = append(jlist, j)

		}
	}

	j := jlist[len(jlist)-1]
	jlist = jlist[:len(jlist)-1]
	go func(jh *job) {
		time.Sleep(time.Millisecond * 10)
		jb.Release(jh)

	}(j)

	for _, j := range jlist {
		jb.Release(j)
	}

}

func TestRSSKill(t *testing.T) {
	bin, err := exec.LookPath("levee")
	if err != nil {
		t.Fatal(err)
	}
	jb := NewJobmanager(
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
	if jb == nil {
		t.Fatal("no job manager")
	}
	for _, m := range jb.Metrics() {
		prometheus.MustRegister(m)

	}
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":8080", nil)

	workC := make(chan int)
	wg := &sync.WaitGroup{}
	for i := 1; i <= 30; i++ {
		wg.Add(1)
		go func() {
			for range workC {
				//fmt.Printf("[TEST] loop count: %d\n", i)
				j, e := jb.Reserve()
				if e != nil {
					t.Fatal(e)
				}
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
