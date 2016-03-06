package controller

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/tile38/controller/log"
)

func (c *Controller) cmdMassInsert(line string) error {
	// massinsert simply forwards a bunch of cmdSets
	var snumCols, snumPoints string
	var cols, objs int
	if line, snumCols = token(line); snumCols == "" {
		return errors.New("invalid number of arguments")
	}
	if line, snumPoints = token(line); snumPoints == "" {
		return errors.New("invalid number of arguments")
	}
	if line != "" {
		return errors.New("invalid number of arguments")
	}
	n, err := strconv.ParseUint(snumCols, 10, 64)
	if err != nil {
		return errors.New("invalid argument '" + snumCols + "'")
	}
	cols = int(n)
	n, err = strconv.ParseUint(snumPoints, 10, 64)
	if err != nil {
		return errors.New("invalid argument '" + snumPoints + "'")
	}
	docmd := func(cmd string) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		_, d, err := c.command(cmd, nil)
		if err != nil {
			return err
		}
		if err := c.writeAOF(cmd, &d); err != nil {
			return err
		}
		return nil
	}
	rand.Seed(time.Now().UnixNano())
	objs = int(n)
	var wg sync.WaitGroup
	var k uint64
	wg.Add(cols)
	for i := 0; i < cols; i++ {
		key := "mi:" + strconv.FormatInt(int64(i), 10)
		go func(key string) {
			defer func() {
				wg.Done()
			}()
			for j := 0; j < objs; j++ {
				id := strconv.FormatInt(int64(j), 10)
				lat, lon := rand.Float64()*180-90, rand.Float64()*360-180
				var line string
				if true {
					fields := fmt.Sprintf("FIELD field %f", rand.Float64()*10)
					line = fmt.Sprintf(`set %s %s %s POINT %f %f`, key, id, fields, lat, lon)
				} else {
					line = fmt.Sprintf(`set %s %s POINT %f %f`, key, id, lat, lon)
				}
				if err := docmd(line); err != nil {
					log.Fatal(err)
					return
				}
				atomic.AddUint64(&k, 1)
				if j%10000 == 10000-1 {
					log.Infof("massinsert: %s %d/%d", key, atomic.LoadUint64(&k), cols*objs)
				}
			}
		}(key)
	}
	wg.Wait()
	log.Infof("massinsert: done %d objects", atomic.LoadUint64(&k))
	return nil
}
