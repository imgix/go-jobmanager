# go-jobmanager

- run a subprocess pool
- configure min and max subprocesses, this library will grow the pool as jobs are needed
- configure maximum RSS, this lib will rotate jobs who's processes exceeds the RSS

jobmanager's are goroutine safe, individual jobs returned by `Reserve()` are not.

## example

the jobmanager needs a `Runner` passed in 

```golang
type cmdTester struct {
	path string
	args []string
	dir  string
}

// impelementes the runner interface
func (c *cmdTester) Run(i uint64) *exec.Cmd {
	return &exec.Cmd{
		Path: c.path,
		Args: c.args,
		Dir:  c.dir,
	}

}


jb := NewJobManager(
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

jb.Run(bookeepIntervalMs)
j := jb.Reserve()
if resp, release, err := j.Communicate([]byte("test command!!!!\n")); err != nil {
	panic(err)
}

fmt.Printf("%v\n", resp)
// library manages buffer pools, so release the bytes when done
release()

// return the job (worker) back to the manager
// any jobs that returned EOF on their stdout pipe will be cycled properly
jb.Release(j)

// shuts down all subprocs
jb.Stop()
```
