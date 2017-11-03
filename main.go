package jobmanager

import (
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jolestar/go-commons-pool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/process"
)

type Runner interface {
	Run(uint64) *exec.Cmd
}

type Initializer interface {
	Init(uint64, Communicator) error
}
type Communicator interface {
	Communicate([]byte) ([]byte, func(), error)
}

type Jobmanager struct {
	opool        *pool.ObjectPool
	reuseCounter prometheus.Counter
	reserveTimer prometheus.Histogram
	collectors   []prometheus.Collector
}

func NewJobmanager(run Runner, namespace, subsystem, jobname string,
	min, max int, maxrss uint64) *Jobmanager {

	type proc struct {
		p   *process.Process
		rss uint64
	}
	jb := &Jobmanager{
		reserveTimer: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:        "worker_reserve_wait",
				Namespace:   namespace,
				Subsystem:   subsystem,
				ConstLabels: prometheus.Labels{"jobname": jobname},
				Help:        "time spent waiting for free subproc",
				Buckets:     []float64{0.000001, 0.00001, 0.001, .10, 1, 5},
			},
		),
		reuseCounter: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name:        "job_reuse",
				Namespace:   namespace,
				Subsystem:   subsystem,
				ConstLabels: prometheus.Labels{"jobname": jobname},
				Help:        "job re-use counter",
			},
		),
	}

	pids := make(map[int32]*proc)
	pidsL := sync.Mutex{}
	go func() {
		for {
			time.Sleep(time.Second * 20)
			pidsL.Lock()
			for _, v := range pids {
				if m, err := v.p.MemoryInfo(); err != nil {
					continue
				} else {
					v.rss = m.RSS
				}
			}
			pidsL.Unlock()

		}
	}()

	//https://godoc.org/github.com/jolestar/go-commons-pool#ObjectPoolConfig
	cfg := pool.ObjectPoolConfig{
		Lifo:               true,
		MaxTotal:           max,
		MaxIdle:            min + min/2,
		MinIdle:            min,
		TestOnCreate:       true,
		TestOnBorrow:       false,
		TestOnReturn:       false,
		TestWhileIdle:      false,
		BlockWhenExhausted: true,
		MaxWaitMillis:      200,

		MinEvictableIdleTimeMillis: 90000,
		NumTestsPerEvictionRun:     -4,
		//TimeBetweenEvictionRunsMillis:
	}
	runTimer := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:        "jobruntime",
			Namespace:   namespace,
			Subsystem:   subsystem,
			ConstLabels: prometheus.Labels{"jobname": jobname},
			Buckets:     []float64{0.00010, 0.001, 0.01, .1, 1, 5, 10},

			Help: "job manager timer",
		},
	)

	var jobid uint64
	factory := pool.NewPooledObjectFactory(
		//create
		func() (interface{}, error) {
			j, e := newjob(atomic.AddUint64(&jobid, 1), run, runTimer)
			if e != nil {
				return nil, e
			}
			pidsL.Lock()
			pids[j.id] = new(proc)
			if pids[j.id].p, e = process.NewProcess(j.id); e != nil {
				pidsL.Unlock()
				return nil, e
			}
			pidsL.Unlock()
			return j, nil

		},
		//destroy
		func(o *pool.PooledObject) error {
			if j, ok := o.Object.(*job); !ok {
				return fmt.Errorf("bad object")
			} else {
				pidsL.Lock()
				delete(pids, j.id)
				pidsL.Unlock()
				j.stop(true)
			}
			return nil
		},
		//validate
		func(o *pool.PooledObject) bool {
			j, _ := o.Object.(*job)
			pidsL.Lock()
			defer pidsL.Unlock()

			if p, ok := pids[j.id]; !ok {
				return true
			} else if p.rss > maxrss {
				return false
			}
			return true
		},
		//activate
		func(o *pool.PooledObject) error { return nil },
		//passivate
		func(o *pool.PooledObject) error { return nil },
	)
	jb.opool = pool.NewObjectPool(factory, &cfg)

	jobStates := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "job_state_transitions",
			Namespace:   namespace,
			Subsystem:   subsystem,
			ConstLabels: prometheus.Labels{"jobname": jobname},
			Help:        "child procs state transitions",
		},
		[]string{"state"},
	)
	go func() {
		for {
			jobStates.WithLabelValues("destroyed").Set(
				float64(jb.opool.GetDestroyedCount()),
			)
			jobStates.WithLabelValues("idle").Set(
				float64(jb.opool.GetNumIdle()),
			)

			jobStates.WithLabelValues("active").Set(
				float64(jb.opool.GetNumActive()),
			)

			time.Sleep(time.Second * 3)
		}
	}()
	jb.opool.PreparePool()
	jb.opool.StartEvictor()

	jb.collectors = []prometheus.Collector{
		runTimer,
		jobStates,
		jb.reserveTimer,
		jb.reuseCounter,
	}

	return jb
}

func (jb *Jobmanager) Stop() {
	jb.opool.Close()
	for {
		if jb.opool.IsClosed() {
			return
		}
		time.Sleep(time.Millisecond * 5)
	}
}

func (jb *Jobmanager) Metrics() []prometheus.Collector {
	return jb.collectors
}

func (jb *Jobmanager) reserve() (*job, error) {

	i, err := jb.opool.BorrowObject()
	if err != nil {
		return nil, err
	}
	j, _ := i.(*job)
	if j.useCount > 0 {
		jb.reuseCounter.Inc()
	}

	return j, nil

}
func (jb *Jobmanager) Reserve() (*job, error) {
	timer := prometheus.NewTimer(
		prometheus.ObserverFunc(func(v float64) {
			jb.reserveTimer.Observe(v)
		},
		),
	)
	defer timer.ObserveDuration()
	return jb.reserve()
}

func (jb *Jobmanager) Release(j *job) {

	if j.Recycle {
		jb.opool.InvalidateObject(j)
	} else {
		j.useCount++

		jb.opool.ReturnObject(j)
	}
}
