package mainboilerplate

import (
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"time"

	log "github.com/sirupsen/logrus"
)

// dump writes the heap and goroutine trace to |w|.
func dump(w io.Writer) {
	pprof.Lookup("heap").WriteTo(w, 1)
	pprof.Lookup("goroutine").WriteTo(w, 1)
}

// profileFile is the target file for CPU profiling.
var profileFile *os.File

// toggleProfiler starts and stops a long-running CPU profile using pprof. The
// profile is written to /var/tmp/profile_${PID}_${TIMESTAMP}.pprof where
// TIMESTAMP represents the epoch time when the profiling session began.
func toggleProfiler() {
	if profileFile == nil {
		var err error

		filename := fmt.Sprintf("/var/tmp/profile_%d_%d.pprof",
			os.Getpid(), time.Now().Unix())

		profileFile, err = os.Create(filename)
		if err == nil {
			pprof.StartCPUProfile(profileFile)
		} else {
			log.WithField("err", err).Error("could not begin CPU profiling")
		}
	} else {
		pprof.StopCPUProfile()
		profileFile.Close()
		profileFile = nil
	}
}
