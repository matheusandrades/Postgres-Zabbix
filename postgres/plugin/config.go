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
	"git.zabbix.com/ap/plugin-support/conf"
	"git.zabbix.com/ap/plugin-support/plugin"
)

// Session struct holds individual options for PostgreSQL connection for each session.
type Session struct {
	// URI is a connection string consisting of a network scheme, a host address and a port or a path to a Unix-socket.
	URI string `conf:"name=Uri,optional"`

	// User of PostgreSQL server.
	User string `conf:"optional"`

	// Password to send to protected PostgreSQL server.
	Password string `conf:"optional"`

	// Database of PostgreSQL server.
	Database string `conf:"optional"`

	// Connection type of PostgreSQL server.
	TLSConnect string `conf:"name=TLSConnect,optional"`

	// Certificate Authority filepath for PostgreSQL server.
	TLSCAFile string `conf:"name=TLSCAFile,optional"`

	// Certificate filepath for PostgreSQL server.
	TLSCertFile string `conf:"name=TLSCertFile,optional"`

	// Key filepath for PostgreSQL server.
	TLSKeyFile string `conf:"name=TLSKeyFile,optional"`
}

// PluginOptions are options for PostgreSQL connection.
type PluginOptions struct {
	plugin.SystemOptions `conf:"optional,name=System"`

	// Timeout is the maximum time in seconds for waiting when a connection has to be established.
	// Default value equals to the global agent timeout.
	Timeout int `conf:"optional,range=1:30"`

	// CallTimeout is the maximum time in seconds for waiting when a request has to be done.
	// Default value equals to the global agent timeout.
	CallTimeout int `conf:"optional,range=1:30"`

	// KeepAlive is a time to wait before unused connections will be closed.
	KeepAlive int `conf:"optional,range=60:900,default=300"`

	// Sessions stores pre-defined named sets of connections settings.
	Sessions map[string]Session `conf:"optional"`

	// CustomQueriesPath is a full pathname of a directory containing *.sql files with custom queries.
	CustomQueriesPath string `conf:"optional"`
}

// Configure implements the Configurator interface.
// Initializes configuration structures.
func (p *Plugin) Configure(global *plugin.GlobalOptions, options interface{}) {
	if err := conf.Unmarshal(options, &p.options); err != nil {
		p.Errf("cannot unmarshal configuration options: %s", err)
	}

	if p.options.Timeout == 0 {
		p.options.Timeout = global.Timeout
	}

	if p.options.CallTimeout == 0 {
		p.options.CallTimeout = global.Timeout
	}
}

// Validate implements the Configurator interface.
// Returns an error if validation of a plugin's configuration is failed.
func (p *Plugin) Validate(options interface{}) error {
	var opts PluginOptions

	return conf.Unmarshal(options, &opts)
}
