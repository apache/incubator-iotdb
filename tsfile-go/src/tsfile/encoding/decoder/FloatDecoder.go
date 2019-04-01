/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package decoder

import (
	_ "bytes"
	"math"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader        *utils.BytesReader
	baseDecoder   Decoder
	maxPointValue float64
}

func (d *FloatDecoder) Init(data []byte) {
	if d.encoding == constant.RLE {
		d.baseDecoder = &IntRleDecoder{dataType: d.dataType}
	} else if d.encoding == constant.TS_2DIFF {
		d.baseDecoder = &IntDeltaDecoder{dataType: d.dataType}
	} else {
		panic("encoding is not supported by FloatDecoder: " + strconv.Itoa(int(d.dataType)))
	}

	d.reader = utils.NewBytesReader(data)

	maxPointNumber := d.reader.ReadUnsignedVarInt()
	if maxPointNumber <= 0 {
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow(10.0, float64(maxPointNumber))
	}

	d.baseDecoder.Init(d.reader.Remaining())
}

func (d *FloatDecoder) HasNext() bool {
	if d.baseDecoder == nil {
		return false
	}
	return d.baseDecoder.HasNext()
}

func (d *FloatDecoder) Next() interface{} {
	value := d.baseDecoder.Next().(int32)
	result := float64(value) / d.maxPointValue

	return float32(result)
}

func NewFloatDecoder(encoding constant.TSEncoding, dataType constant.TSDataType) *FloatDecoder {
	return &FloatDecoder{encoding: encoding, dataType: dataType}
}
