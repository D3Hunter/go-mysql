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
	"os"
	"time"

	"github.com/siddontang/go-log/log"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func main() {
	host := os.Args[1]
	parseEvent := os.Args[1] == "true"
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID:       100,
		Flavor:         "mysql",
		Host:           host,
		Port:           3306,
		User:           "root",
		Password:       "123456",
		RawModeEnabled: true,
		ParseEvent:     parseEvent,
	}

	cfg.DumpCommandFlag = replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT

	log.Info("starting")
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()
	_, err := syncer.StartSync(mysql.Position{Pos: 4})
	if err != nil {
		panic(err)
	}

	start := time.Now()
	lastTime := start
	lastBytes := syncer.BytesRead.Load()
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			currTime := time.Now()
			currBytes := syncer.BytesRead.Load()
			log.Infof("download speed: total=%.2f MB/s, curr=%.2f MB/s",
				float64(currBytes)/1024.0/1024.0/currTime.Sub(start).Seconds(),
				float64(currBytes-lastBytes)/1024.0/1024.0/currTime.Sub(lastTime).Seconds())
			lastBytes = currBytes
			lastTime = currTime
		}
	}
}
