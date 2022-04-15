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
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			d := time.Since(start)
			log.Infof("download speed: %.2f MB/s", float64(syncer.BytesRead.Load())/1024.0/1024.0/d.Seconds())
		}
	}
}
