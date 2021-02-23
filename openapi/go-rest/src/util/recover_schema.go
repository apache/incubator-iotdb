/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package util

func RecoverSchema() {
	dataSet, _ := Session.ExecuteQueryStatement("select * from root.system_p.label_info", 1000)
	for {
		haseNext, _ := dataSet.Next()
		if haseNext {
			Timestamp++
			record, _ := dataSet.GetRowRecord()
			metricName := record.GetFields()[0].GetText()
			tagName := record.GetFields()[1].GetText()
			tagOrder := record.GetFields()[2].GetInt32()

			if MetricTagOrder[metricName] == nil {
				MetricTagOrder[metricName] = make(map[string]int32)
				MetricOrderTag[metricName] = make(map[int32]string)
			}

			MetricTagOrder[metricName][tagName] = tagOrder
			MetricOrderTag[metricName][tagOrder] = tagName
		} else {
			break
		}
	}
}
