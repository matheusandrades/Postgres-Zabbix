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
	"fmt"
)

const (
	pingFailed = 0
	pingOk     = 1
)

// pingHandler queries 'SELECT 1' and returns pingOk if a connection is alive or pingFailed otherwise.
func pingHandler(ctx context.Context, conn PostgresClient,
	_ string, _ map[string]string, _ ...string) (interface{}, error) {
	var res int

	row, err := conn.QueryRow(ctx, fmt.Sprintf("SELECT %d", pingOk))
	if err != nil {
		return pingFailed, nil
	}

	err = row.Scan(&res)

	if err != nil || res != pingOk {
		return pingFailed, nil
	}

	return pingOk, nil
}