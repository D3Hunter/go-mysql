// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.uber.org/atomic"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func main() {
	host := os.Args[1]
	rawMode := os.Args[2] == "raw"
	gtidMode := os.Args[3] == "gtid"
	chSize, _ := strconv.Atoi(os.Args[4])
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID:       100,
		Flavor:         "mysql",
		Host:           host,
		Port:           3306,
		User:           "root",
		Password:       "123456",
		RawModeEnabled: rawMode,
		VerifyChecksum: true,
	}

	cfg.DumpCommandFlag = replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()
	var streamer *replication.BinlogStreamer
	var err error
	if gtidMode {
		set, _ := mysql.ParseMysqlGTIDSet("")
		streamer, err = syncer.StartSyncGTID(set)
	} else {
		streamer, err = syncer.StartSync(mysql.Position{Pos: 4})
	}
	if err != nil {
		panic(err)
	}

	var bytesRead atomic.Int64
	go func() {
		start := time.Now()
		lastTime := start
		lastBytes := bytesRead.Load()
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				currTime := time.Now()
				currBytes := bytesRead.Load()
				fmt.Printf("%v: %.2f MB/s\n",
					//float64(currBytes)/1024.0/1024.0/currTime.Sub(start).Seconds(),
					currTime.Format("2006-01-02 15:04:05"),
					float64(currBytes-lastBytes)/1024.0/1024.0/currTime.Sub(lastTime).Seconds())
				lastBytes = currBytes
				lastTime = currTime
			}
		}
	}()

	chs := make([]chan *replication.BinlogEvent, chSize)
	for i := 0; i < chSize; i++ {
		chs[i] = make(chan *replication.BinlogEvent, 1024)
	}
	for i := 0; i < chSize-1; i++ {
		go func(i int) {
			for e := range chs[i] {
				chs[i+1] <- e
			}
		}(i)
	}
	go func() {
		for e := range chs[chSize-1] {
			bytesRead.Add(int64(len(e.RawData)))
		}
	}()
	for {
		e, _ := streamer.GetEvent(context.Background())
		chs[0] <- e
	}
}
