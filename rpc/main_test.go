// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// NewTestContext returns a rpc.Context for testing.
// It is meant to be used by rpc clients.
func NewTestContext(clock *hlc.Clock, stopper *util.Stopper) *Context {
	if clock == nil {
		clock = hlc.NewClock(hlc.UnixNano)
	}
	return NewContext(testutils.NewTestBaseContext(), clock, stopper)
}

// NewServerTestContext returns a rpc.Context for testing.
// It is meant to be used by rpc servers.
func NewServerTestContext(clock *hlc.Clock, stopper *util.Stopper) *Context {
	if clock == nil {
		clock = hlc.NewClock(hlc.UnixNano)
	}
	return NewContext(testutils.NewServerTestBaseContext(), clock, stopper)
}

func init() {
	security.SetReadFileFn(securitytest.Asset)
}

//go:generate ../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	leaktest.TestMainWithLeakCheck(m)
}
