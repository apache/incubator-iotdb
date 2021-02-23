//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

module github.com/apache/iotdb/openapi/go-rest/src

go 1.13

replace github.com/iotdbrest => ../target/gen/iotdbrest

require (
	github.com/apache/iotdb-client-go v0.0.0-20210121135451-b5e80e0720c8
	github.com/cespare/xxhash v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.2
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/iotdbrest v0.0.0-00010101000000-000000000000
	github.com/prometheus/prometheus v2.5.0+incompatible
	google.golang.org/genproto v0.0.0-20210122163508-8081c04a3579 // indirect
	google.golang.org/grpc v1.35.0 // indirect
	gopkg.in/yaml.v2 v2.4.0
)
