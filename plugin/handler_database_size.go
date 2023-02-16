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

	"git.zabbix.com/ap/plugin-support/zbxerr"
	"github.com/jackc/pgx/v4"
)

// databaseSizeHandler gets info about count and size of archive files and returns JSON if all is OK or nil otherwise.
func databaseSizeHandler(ctx context.Context, conn PostgresClient,
	_ string, params map[string]string, _ ...string) (interface{}, error) {
	var countSize int64

	query := `SELECT pg_database_size(datname::text)
		FROM pg_catalog.pg_database
   		WHERE datistemplate = false
			 AND datname = $1;`

	row, err := conn.QueryRow(ctx, query, params["Database"])
	if err != nil {
		return nil, zbxerr.ErrorCannotFetchData.Wrap(err)
	}

	err = row.Scan(&countSize)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, zbxerr.ErrorEmptyResult.Wrap(err)
		}

		return nil, zbxerr.ErrorCannotFetchData.Wrap(err)
	}

	return countSize, nil
}
