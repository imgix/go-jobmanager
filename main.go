package jobmanager

import (
	"os"
	"os/exec"
	"sync"
	"time"

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
	minjobs, maxjobs int64
	maxrss           uint64
	blockedjobs      int32
	jobTimeout       time.Duration

	logger          func([]byte)
	runner          Runner
	allocC          chan *job
	releaseC        chan *job
	runningC        chan struct{}
	doneC           chan struct{}
	initWg          *sync.WaitGroup
	jobactions      *prometheus.CounterVec
	runningChildren prometheus.Gauge
	runTimer        *prometheus.HistogramVec
	reserveTimer    *prometheus.HistogramVec
}

func NewJobManagerWithTimeout(run Runner, namespace, subsystem, jobname string,
	min, max int64, maxrss uint64, d time.Duration) (*Jobmanager, error) {
	if jb, err := NewJobManager(
		run, namespace, subsystem,
		jobname, min, max, maxrss,
	); err != nil {
		return nil, err
	} else {
		jb.jobTimeout = d
	}
	return nil, nil

}
func NewJobManager(run Runner, namespace, subsystem, jobname string,
	min, max int64, maxrss uint64) (*Jobmanager, error) {
	if subsystem == "" {
		subsystem = "Jobmanager"
	}

	jb := &Jobmanager{
		minjobs:    min,
		maxjobs:    max,
		maxrss:     maxrss,
		runner:     run,
		allocC:     make(chan *job),
		releaseC:   make(chan *job),
		runningC:   make(chan struct{}),
		doneC:      make(chan struct{}),
		initWg:     &sync.WaitGroup{},
		logger:     func(b []byte) { os.Stderr.Write(b) },
		jobTimeout: time.Second * 2,
		runningChildren: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:        "childproc_count",
				Namespace:   namespace,
				Subsystem:   subsystem,
				ConstLabels: prometheus.Labels{"jobname": jobname},
				Help:        "number of child processes",
			},
		),

		jobactions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "jobactions",
				Namespace:   namespace,
				Subsystem:   subsystem,
				ConstLabels: prometheus.Labels{"jobname": jobname},
				Help:        "job manager actions taken",
			},
			[]string{"action"},
		),
		runTimer: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "jobruntime",
				Namespace:   namespace,
				Subsystem:   subsystem,
				ConstLabels: prometheus.Labels{"jobname": jobname},

				//Buckets: prometheus.LinearBuckets( 0.00010, 0.001, 7,),

				Help: "job manager timer",
				Buckets: prometheus.ExponentialBuckets(
					0.00010, 3.0, 7,
				),
			},
			[]string{"phase"},
		),
		reserveTimer: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "worker_wait",
				Namespace:   namespace,
				Subsystem:   subsystem,
				ConstLabels: prometheus.Labels{"jobname": jobname},
				/*
					Buckets: prometheus.LinearBuckets(
						0.000020, 0.001, 10,
					),
				*/

				Help: "job manager timer",
				Buckets: prometheus.ExponentialBuckets(
					0.0000003, 3.0, 7,
				),
			},
			[]string{"phase"},
		),
	}

	if j, err := newjob(0, jb.runner, jb.runTimer); err != nil {
		return nil, err
	} else {
		j.stop(true)
	}
	return jb, nil

}

func (jb *Jobmanager) Metrics() []prometheus.Collector {

	return []prometheus.Collector{
		jb.runningChildren,
		jb.jobactions,
		jb.runTimer,
		jb.reserveTimer,
	}
}

func (jb *Jobmanager) reserve() *job {

	return <-jb.allocC
}
func (jb *Jobmanager) Reserve() *job {
	timer := prometheus.NewTimer(
		prometheus.ObserverFunc(func(v float64) {
			jb.reserveTimer.WithLabelValues("reserve").Observe(v)
		},
		),
	)
	defer timer.ObserveDuration()
	return jb.reserve()
}

func (jb *Jobmanager) Release(j *job) {
	jb.releaseC <- j
}

func (jb *Jobmanager) run(bookinterval time.Duration) {
	defer close(jb.doneC)
	running := true

	var (
		jobpoolFree    []*job
		jobmapReserved = make(map[int32]*job)
		cmdMap         = make(map[int32]*job)
		procsMutex     = &sync.Mutex{}
		pidRss         = make(map[int32]uint64)
		jobID          = uint64(1)
	)
	currentjobs := func() int64 {
		i := len(jobpoolFree)
		for range jobmapReserved {
			i++
		}
		return int64(i)
	}

	for int64(len(jobpoolFree)) != jb.minjobs {
		if j, err := newjob(jobID, jb.runner, jb.runTimer); err != nil {
			jb.logger([]byte(err.Error()))
			time.Sleep(time.Millisecond * 200)
			continue
		} else {
			jb.jobactions.WithLabelValues("new_job").Add(1)
			jobpoolFree = append(jobpoolFree, j)
		}
		jobID++

	}
	type pidRSS struct {
		rss uint64
		pid int32
	}

	getProcs := func() []*process.Process {
		procs := make([]*process.Process, 0)

		procsMutex.Lock()
		for _, j := range cmdMap {

			if p, err := process.NewProcess(j.id); err != nil {
				continue
			} else {
				procs = append(procs, p)
			}

		}
		procsMutex.Unlock()
		return procs
	}

	rssResp := make(chan pidRSS)
	go func() {
		for {
			time.Sleep(bookinterval)
			procs := getProcs()
			for _, p := range procs {
				if m, err := p.MemoryInfo(); err != nil {
					continue
				} else {

					rssResp <- pidRSS{
						pid: p.Pid,
						rss: m.RSS,
					}
				}
			}

		}
	}()

	bookkeep := func(t time.Time) []*job {
		var tmp []*job
		for i, j := range jobpoolFree {
			if j.start.Add(time.Second * 60).Before(t) {
				j.stop(true)
				jb.jobactions.WithLabelValues("reap_idle").Add(1)
				delete(pidRss, j.id)

				procsMutex.Lock()
				delete(cmdMap, j.id)
				procsMutex.Unlock()
			} else if rss, ok := pidRss[j.id]; ok && rss > jb.maxrss {
				jb.jobactions.WithLabelValues("rss_kill").Add(1)

				j.stop(true)
				delete(pidRss, j.id)

				procsMutex.Lock()
				delete(cmdMap, j.id)
				procsMutex.Unlock()

			} else {
				tmp = append(tmp, j)
				j.useCount = 0
			}
			jobpoolFree[i] = nil
		}
		now := time.Now()
		var timeoutJobs []*job
		for _, j := range jobmapReserved {
			if now.After(j.start.Add(jb.jobTimeout)) {
				timeoutJobs = append(timeoutJobs, j)

			}
		}
		for _, j := range timeoutJobs {
			jb.jobactions.WithLabelValues("job_timeout").Add(1)
			j.stop(true)
			delete(jobmapReserved, j.id)
			delete(pidRss, j.id)
			procsMutex.Lock()
			delete(cmdMap, j.id)
			procsMutex.Unlock()
		}
		return tmp

	}

	releaseJob := func(j *job, t time.Time) *job {
		delete(jobmapReserved, j.id)
		if j.Recycle {
			jb.jobactions.WithLabelValues("recycle").Add(1)
		} else if pidRss[j.id] > jb.maxrss {
			j.Recycle = true
			jb.jobactions.WithLabelValues("rss_kill").Add(1)

		} else if !j.running {
			j.Recycle = true
			jb.jobactions.WithLabelValues("died").Add(1)
		}

		if j.Recycle {
			j.stop(true)
			delete(pidRss, j.id)
			procsMutex.Lock()
			delete(cmdMap, j.id)
			procsMutex.Unlock()
			return nil

		}
		j.start = t
		return j
	}

	jb.initWg.Done()
	bookkeepC := time.Tick(bookinterval)
	var now time.Time
	for running {
		if len(jobpoolFree) == 0 {
			if currentjobs() >= jb.maxjobs {
				jb.jobactions.WithLabelValues("wait_maxjobs").Add(1)
				select {
				case j := <-jb.releaseC:
					if j = releaseJob(j, now); j != nil {
						jobpoolFree = append(jobpoolFree, j)
					}
				case <-bookkeepC:
					jobpoolFree = bookkeep(now)

				}
				continue

			} else if jsub, err := newjob(jobID, jb.runner, jb.runTimer); err != nil {
				jb.jobactions.WithLabelValues("job_create_err").Add(1)
				time.Sleep(time.Millisecond * 5)
				continue

			} else {

				jb.jobactions.WithLabelValues("new_job").Add(1)
				jobpoolFree = append(jobpoolFree, jsub)
				jobID++

				procsMutex.Lock()
				cmdMap[jsub.id] = jsub
				procsMutex.Unlock()

			}
		}

		select {
		case _, running = <-jb.runningC:

		case j := <-jb.releaseC:
			if j = releaseJob(j, now); j != nil {
				jobpoolFree = append(jobpoolFree, j)
			}

		case pRss := <-rssResp:
			pidRss[pRss.pid] = pRss.rss

		case jb.allocC <- jobpoolFree[len(jobpoolFree)-1]:
			j := jobpoolFree[len(jobpoolFree)-1]
			jobpoolFree[len(jobpoolFree)-1] = nil
			jobpoolFree = jobpoolFree[:len(jobpoolFree)-1]
			j.start = time.Now()
			j.useCount++
			j.totCount++
			if j.totCount > 1 {
				jb.jobactions.WithLabelValues("reuse").Add(1)
			}
			jobmapReserved[j.id] = j

		case now = <-bookkeepC:
			jobpoolFree = bookkeep(now)
			// current includes the one ready for alloc
			jb.runningChildren.Set(float64(currentjobs()))
		}

	}
	for _, j := range jobpoolFree {
		j.stop(false)
	}
	jobpoolFree = jobpoolFree[:0]
	for currentjobs() > 0 {
		j := <-jb.releaseC
		j.stop(false)
	}
}

func (jb *Jobmanager) Run(bookinterval time.Duration) {
	jb.initWg.Add(1)
	go jb.run(bookinterval)
	jb.initWg.Wait()
}

func (jb *Jobmanager) Stop() {
	close(jb.runningC)
	<-jb.doneC
}
