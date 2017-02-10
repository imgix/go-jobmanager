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

type Jobmanager struct {
	minjobs, maxjobs, currentjobs int64
	maxrss                        uint64
	blockedjobs                   int32
	jobTimeout                    time.Duration

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

func NewJobManager(run Runner, namespace, subsystem, jobname string,
	min, max int64, maxrss uint64) *Jobmanager {
	if subsystem == "" {
		subsystem = "Jobmanager"
	}
	return &Jobmanager{
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
				ConstLabels: prometheus.Labels{jobname: jobname},
				Help:        "number of child processes",
			},
		),

		jobactions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "jobactions",
				Namespace:   namespace,
				Subsystem:   subsystem,
				ConstLabels: prometheus.Labels{jobname: jobname},
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
		err            error
		jobpoolFree    []*job
		jobmapReserved = make(map[int32]*job)
		cmdMap         = make(map[int32]*job)
		procsMutex     = &sync.Mutex{}
		pidRss         = make(map[int32]uint64)
	)

	for int64(len(jobpoolFree)) != jb.minjobs {
		if j, err := newjob(0, jb.runner, jb.runTimer); err != nil {
			jb.logger([]byte(err.Error()))
			time.Sleep(time.Millisecond * 200)
			continue
		} else {
			jb.currentjobs++
			jb.jobactions.WithLabelValues("new_job").Add(1)
			jobpoolFree = append(jobpoolFree, j)
		}

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

	bookkeep := func() {
		var tmp []*job
		for i, j := range jobpoolFree {
			if j.useCount < 2 {
				j.stop(true)
				jb.jobactions.WithLabelValues("reap_idle").Add(1)
				jb.currentjobs--
				delete(pidRss, j.id)

				procsMutex.Lock()
				delete(cmdMap, j.id)
				procsMutex.Unlock()
			} else if rss, ok := pidRss[j.id]; ok && rss > jb.maxrss {
				jb.jobactions.WithLabelValues("rss_kill").Add(1)

				j.stop(true)
				jb.currentjobs--
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
		jobpoolFree = tmp

		now := time.Now()
		for _, j := range jobmapReserved {
			if now.After(j.start.Add(jb.jobTimeout)) && jb.currentjobs > 0 {

				jb.jobactions.WithLabelValues("job_timeout").Add(1)
				j.stop(true)
				delete(jobmapReserved, j.id)
				jb.currentjobs--
				delete(pidRss, j.id)
				procsMutex.Lock()
				delete(cmdMap, j.id)
				procsMutex.Unlock()
			}
		}

	}
	getjob := func() (*job, error) {
		var jsub *job
		if len(jobpoolFree) < 1 {
			if jb.currentjobs >= jb.maxjobs {
				t := time.NewTicker(time.Millisecond * 20)
				defer t.Stop()
				jb.jobactions.WithLabelValues("wait_maxjobs").Add(1)
				for {
					select {
					case jsub = <-jb.releaseC:
						delete(jobmapReserved, jsub.id)
						if jsub.running {
							return jsub, nil
						}

						jb.jobactions.WithLabelValues("died").Add(1)
						delete(pidRss, jsub.id)
						procsMutex.Lock()
						delete(cmdMap, jsub.id)
						procsMutex.Unlock()

					case <-t.C:
						bookkeep()
					}
				}

			}
			if jsub, err = newjob(0, jb.runner, jb.runTimer); err != nil {
				jb.jobactions.WithLabelValues("job_create_err").Add(1)
				return nil, err
			}

			jb.jobactions.WithLabelValues("new_job").Add(1)
			pidRss[jsub.id] = 0
			jb.currentjobs++
			procsMutex.Lock()
			cmdMap[jsub.id] = jsub
			procsMutex.Unlock()
			return jsub, nil

		}
		// pop
		jsub = jobpoolFree[len(jobpoolFree)-1]
		jobpoolFree[len(jobpoolFree)-1] = nil
		jobpoolFree = jobpoolFree[:len(jobpoolFree)-1]
		return jsub, nil

	}

	var allocJ, retJ *job
	if allocJ, err = getjob(); err != nil {
		panic(err)
	}

	jb.initWg.Done()
	bookkeepC := time.Tick(bookinterval)
	for running {

		if _, ok := jobmapReserved[allocJ.id]; ok {
			if allocJ, err = getjob(); err != nil {
				jb.logger([]byte(err.Error()))
				time.Sleep(time.Millisecond * 200)
				continue
			}
		}

		select {
		case _, running = <-jb.runningC:

			if _, ok := jobmapReserved[allocJ.id]; !ok {
				retJ.stop(false)
				jb.jobactions.WithLabelValues("untracked_return").Add(1)

				jb.currentjobs--
				procsMutex.Lock()
				delete(cmdMap, retJ.id)
				procsMutex.Unlock()
			}
		case retJ = <-jb.releaseC:
			delete(jobmapReserved, retJ.id)
			retire := false
			if pidRss[retJ.id] > jb.maxrss {
				retire = true
				jb.jobactions.WithLabelValues("rss_kill").Add(1)

			} else if !retJ.running {
				jb.jobactions.WithLabelValues("died").Add(1)
				retire = true
			}

			if retire {
				retJ.stop(true)
				jb.currentjobs--
				delete(pidRss, retJ.id)

				procsMutex.Lock()
				delete(cmdMap, retJ.id)
				procsMutex.Unlock()

			} else {
				jobpoolFree = append(jobpoolFree, retJ)
			}

		case pRss := <-rssResp:
			pidRss[pRss.pid] = pRss.rss

		case jb.allocC <- allocJ:
			allocJ.start = time.Now()
			allocJ.useCount++
			allocJ.totCount++
			if allocJ.totCount > 1 {
				jb.jobactions.WithLabelValues("reuse").Add(1)
			}

			jobmapReserved[allocJ.id] = allocJ

		case <-bookkeepC:
			bookkeep()
			jb.runningChildren.Set(float64(jb.currentjobs))
		}

	}
	for _, j := range jobpoolFree {
		j.stop(false)
		jb.currentjobs--
	}

	for jb.currentjobs > 0 {
		retJ = <-jb.releaseC

		retJ.stop(false)
		jb.currentjobs--
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
