package controller

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

const useRandField = true

func (c *Controller) cmdMassInsert(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var snumCols, snumPoints string
	var cols, objs int
	var ok bool
	if vs, snumCols, ok = tokenval(vs); !ok || snumCols == "" {
		return "", errInvalidNumberOfArguments
	}
	if vs, snumPoints, ok = tokenval(vs); !ok || snumPoints == "" {
		return "", errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return "", errors.New("invalid number of arguments")
	}
	n, err := strconv.ParseUint(snumCols, 10, 64)
	if err != nil {
		return "", errInvalidArgument(snumCols)
	}
	cols = int(n)
	n, err = strconv.ParseUint(snumPoints, 10, 64)
	if err != nil {
		return "", errInvalidArgument(snumPoints)
	}
	docmd := func(values []resp.Value) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		nmsg := &server.Message{}
		*nmsg = *msg
		nmsg.Values = values
		nmsg.Command = strings.ToLower(values[0].String())

		_, d, err := c.command(nmsg, nil)
		if err != nil {
			return err
		}
		if err := c.writeAOF(resp.ArrayValue(nmsg.Values), &d); err != nil {
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
				values := make([]resp.Value, 0, 16)
				values = append(values, resp.StringValue("set"), resp.StringValue(key), resp.StringValue(id))
				if useRandField {
					values = append(values, resp.StringValue("FIELD"), resp.StringValue("field"), resp.FloatValue(rand.Float64()*10))
				}
				values = append(values, resp.StringValue("POINT"), resp.FloatValue(lat), resp.FloatValue(lon))
				if err := docmd(values); err != nil {
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
	return server.OKMessage(msg, start), nil
}
