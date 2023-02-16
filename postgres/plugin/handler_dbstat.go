/*
** Zabbix
** Copyright 2001-2022 Zabbix SIA
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
**/

package plugin

import (
	"context"
	"errors"
	"fmt"

	"git.zabbix.com/ap/plugin-support/zbxerr"
	"github.com/jackc/pgx/v4"
)

const pgVersionWithChecksum = 120000

// dbStatHandler executes select from pg_catalog.pg_stat_database
// command for each database and returns JSON if all is OK or nil otherwise.
func dbStatHandler(ctx context.Context, conn PostgresClient,
	key string, _ map[string]string, _ ...string) (interface{}, error) {
	var statJSON, query string

	switch key {
	case keyDBStatSum:
		query = `
  SELECT row_to_json (T)
    FROM  (
      SELECT
        sum(numbackends) as numbackends
      , sum(xact_commit) as xact_commit
      , sum(xact_rollback) as xact_rollback
      , sum(blks_read) as blks_read
      , sum(blks_hit) as blks_hit
      , sum(tup_returned) as tup_returned
      , sum(tup_fetched) as tup_fetched
      , sum(tup_inserted) as tup_inserted
      , sum(tup_updated) as tup_updated
      , sum(tup_deleted) as tup_deleted
      , sum(conflicts) as conflicts
      , sum(temp_files) as temp_files
      , sum(temp_bytes) as temp_bytes
      , sum(deadlocks) as deadlocks
      , %s as checksum_failures
      , sum(blk_read_time) as blk_read_time
      , sum(blk_write_time) as blk_write_time
      FROM pg_catalog.pg_stat_database
    ) T ;`
		if conn.PostgresVersion() >= pgVersionWithChecksum {
			query = fmt.Sprintf(query, "sum(COALESCE(checksum_failures, 0))")
		} else {
			query = fmt.Sprintf(query, "null")
		}

	case keyDBStat:
		query = `
  SELECT json_object_agg(coalesce (datname,'null'), row_to_json(T))
    FROM  (
      SELECT
        datname
      , numbackends as numbackends
      , xact_commit as xact_commit
      , xact_rollback as xact_rollback
      , blks_read as blks_read
      , blks_hit as blks_hit
      , tup_returned as tup_returned
      , tup_fetched as tup_fetched
      , tup_inserted as tup_inserted
      , tup_updated as tup_updated
      , tup_deleted as tup_deleted
      , conflicts as conflicts
      , temp_files as temp_files
      , temp_bytes as temp_bytes
      , deadlocks as deadlocks
      , %s as checksum_failures
      , blk_read_time as blk_read_time
      , blk_write_time as blk_write_time
      FROM pg_catalog.pg_stat_database
    ) T ;`
		if conn.PostgresVersion() >= pgVersionWithChecksum {
			query = fmt.Sprintf(query, "COALESCE(checksum_failures, 0)")
		} else {
			query = fmt.Sprintf(query, "null")
		}
	}

	row, err := conn.QueryRow(ctx, query)
	if err != nil {
		return nil, zbxerr.ErrorCannotFetchData.Wrap(err)
	}

	err = row.Scan(&statJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, zbxerr.ErrorEmptyResult.Wrap(err)
		}

		return nil, zbxerr.ErrorCannotFetchData.Wrap(err)
	}

	return statJSON, nil
}
