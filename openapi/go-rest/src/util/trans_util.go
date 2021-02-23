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

import (
	"github.com/apache/iotdb-client-go/client"
	"github.com/cespare/xxhash"
	"github.com/prometheus/prometheus/prompb"
	"strconv"
	"strings"
)

// Remove "" out of node in column
func RemoveDoubleQuotes(column string) []string {
	var start,end,num int
	var nodes []string
	for index, ch2 := range column {
		if string(ch2) == "\"" {
			if string(column[index-1]) != "\\"{
				num ++
			} else {
				continue
			}

			if num == 1 {
				start = index
			} else if num == 2 {
				end = index
				num = 0
				node := column[start+1:end]
				node = strings.Replace(node,"\\","",-1)
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// Trans IoTDB column to prometheus timeseries
func TransColumnToPromTimeseries(columnNodes []string, promLabels []*prompb.Label) []*prompb.Label {
	label := prompb.Label{
		Name:  MetricKey,
		Value: columnNodes[0],
	}
	promLabels = append(promLabels, &label)
	orderLabel := MetricOrderTag["\""+columnNodes[0]+"\""]
	for j := 1; j < len(columnNodes); j++ {
		label := prompb.Label{
			Name: orderLabel[int32(j)] ,
			Value: columnNodes[j],
		}
		promLabels = append(promLabels, &label)

	}
	return promLabels
}

// Trans IoTDB dataSet to prometheus result and add result to results([]result)
func TransResultToPrometheus(sessionDataSet *client.SessionDataSet, promQueryResults []*prompb.QueryResult) []*prompb.QueryResult {
	for next, err := sessionDataSet.Next(); err == nil && next; next, err = sessionDataSet.Next() {
		sample := prompb.Sample{
			Value:     0,
			Timestamp: 0,
		}
		if !sessionDataSet.IsIgnoreTimeStamp() {
			sample.Timestamp = sessionDataSet.GetInt64(client.TimestampColumnName)
		}
		var proTimeseries []*prompb.TimeSeries
		for i := 0; i < sessionDataSet.GetColumnCount(); i++ {
			columnName := sessionDataSet.GetColumnName(i)
			v := sessionDataSet.GetValue(columnName)
			var labels []*prompb.Label
			if v == nil {
				continue
			} else {
				columnNodes := RemoveDoubleQuotes(sessionDataSet.GetColumnName(i))
				labels = TransColumnToPromTimeseries(columnNodes, labels)
				sample.Value= v.(float64)
				promTimes := prompb.TimeSeries{
					Labels:  labels,
					Samples: []prompb.Sample{sample},
				}
				proTimeseries = append(proTimeseries, &promTimes)
			}
		}

		promResult := prompb.QueryResult{Timeseries: proTimeseries}
		promQueryResults = append(promQueryResults,&promResult)
	}
	return promQueryResults
}

// Trans metricName and labelValue to sensor and deviceId when execute write
func TransWriteSchema(tagKeyValues map[string]string, metric string, sgName string) (dvId string, sensor string) {
	paths := make(map[int32]string)
	tagOrderMap := MetricTagOrder[metric]
	orderTagMap := MetricOrderTag[metric]
	if tagOrderMap == nil {
		// it is a new metric
		tagOrderMap = make(map[string]int32)
		orderTagMap = make(map[int32]string)
	}
	for k, v := range tagKeyValues {
		tagOrder := tagOrderMap[k]
		if tagOrder == 0 {
			// it is a new tag
			tagOrderMap[k] = int32(len(tagOrderMap) + 1)
			orderTagMap[int32(len(tagOrderMap) + 1)] = k
			measurements := []string{"metric_name", "tag_name", "tag_order"}
			dataTypes := []client.TSDataType{client.TEXT, client.TEXT, client.INT32}
			values := []interface{}{metric, k, tagOrderMap[k]}
			Session.InsertRecord(DeviceId, measurements, dataTypes, values, Timestamp)

			Timestamp++
			paths[tagOrderMap[k]] = v
		} else {
			paths[tagOrder] = v
		}
	}
	MetricTagOrder[metric] = tagOrderMap
	MetricOrderTag[metric] = orderTagMap
	dvId = Root + NodesSeparator + sgName + NodesSeparator + metric
	for i := 1; i <= len(paths)-1; i++ {
		if paths[int32(i)] == NullString {
			paths[int32(i)] = Placeholder
		}
		dvId += NodesSeparator + paths[int32(i)]
	}
	sensor = paths[int32(len(paths))]
	return dvId, sensor
}

// Trans metricName and labelValue to sensor and deviceId when execute query
func TransReadSchema(tagKeyValues map[string]string, metric string, sgName string) (dvId string, sensor string) {
	paths := make(map[int32]string)
	tagOrderMap := MetricTagOrder[metric]
	for k, v := range tagKeyValues {
		tagOrder := tagOrderMap[k]
		if tagOrder == 0 {
			return NullString, NullString
		} else {
			paths[tagOrder] = v
		}
	}
	dvId = Root + NodesSeparator + sgName + NodesSeparator + metric
	if len(paths) == 0 {
		sensor = Wildcard
	} else {
		for i := 1; i <= len(paths)-1; i++ {
			if paths[int32(i)] == NullString {
				paths[int32(i)] = Wildcard
			}
			dvId = dvId + NodesSeparator + paths[int32(i)]
		}
		sensor = paths[int32(len(paths))]
	}
	return dvId, sensor
}

// Add "" out of metricName and labelValue
func AddDoubleQuotes(labelName string, labelValue string , metricName string, tagKeyValues map[string]string) (string,map[string]string) {
	labelValue = strings.Replace(labelValue, "\"","\\\"",-1)
	if labelName == MetricKey {
		metricName = "\"" +labelValue + "\""
	} else {
		tagKeyValues[labelName] = "\"" + labelValue + "\""
	}
	return metricName, tagKeyValues
}

// Trans to PointQuerySQL through sensor, deviceId and time
func TransToPointQuery(sensor string, dvId string, start int64, end int64) string {
	sql := Select + sensor + From+ dvId + Where+ Time + Ge + strconv.Itoa(int(start)) + And + Time + Le +
		strconv.Itoa(int(end))
	return sql
}

// Trans metricName to storage group
func TransMetricToSg(metricName string) string {
	sgNum := strconv.Itoa(int(xxhash.Sum64String(metricName)%(uint64(Config.Sg))))
	sgName := SgPrefix + sgNum
	return sgName
}