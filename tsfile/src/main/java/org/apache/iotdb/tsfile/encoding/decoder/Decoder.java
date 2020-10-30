/*
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

package org.apache.iotdb.tsfile.encoding.decoder;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class Decoder {

  private static final String ERROR_MSG = "Decoder not found: %s , DataType is : %s";

  private TSEncoding type;

  public Decoder(TSEncoding type) {
    this.type = type;
  }

  public void setType(TSEncoding type) {
    this.type = type;
  }

  public TSEncoding getType() {
    return type;
  }

  /**
   * get Decoder object by type.
   *
   * @param type TSEncoding type
   * @param dataType TSDataType
   * @return Decoder object
   */
  public static Decoder getDecoderByType(TSEncoding type, TSDataType dataType) {
    // PLA and DFT encoding are not supported in current version
    if (type == TSEncoding.PLAIN) {
      return new PlainDecoder(EndianType.BIG_ENDIAN);
    } else if (type == TSEncoding.RLE) {
      switch (dataType) {
        case BOOLEAN:
        case INT32:
          return new IntRleDecoder(EndianType.BIG_ENDIAN);
        case INT64:
          return new LongRleDecoder(EndianType.BIG_ENDIAN);
        case FLOAT:
        case DOUBLE:
          return new FloatDecoder(TSEncoding.valueOf(type.toString()), dataType);
        default:
          throw new TsFileDecodingException(String.format(ERROR_MSG, type, dataType));
      }
    } else if (type == TSEncoding.TS_2DIFF) {
      switch (dataType) {
        case INT32:
          return new DeltaBinaryDecoder.IntDeltaDecoder();
        case INT64:
          return new DeltaBinaryDecoder.LongDeltaDecoder();
        case FLOAT:
        case DOUBLE:
          return new FloatDecoder(TSEncoding.valueOf(type.toString()), dataType);
        default:
          throw new TsFileDecodingException(
              String.format(ERROR_MSG, type, dataType));
      }
    } else if (type == TSEncoding.GORILLA) {
      switch (dataType) {
        case FLOAT:
          return new SinglePrecisionDecoder();
        case DOUBLE:
          return new DoublePrecisionDecoder();
        default:
          throw new TsFileDecodingException(
              String.format(ERROR_MSG, type, dataType));
      }
    } else if(type == TSEncoding.DIFF) {
      switch (dataType) {
        case INT32:
          return new DiffDecoder.IntDeltaDecoder();
        case INT64:
          return new DiffDecoder.LongDeltaDecoder();
        default:
          throw new TsFileDecodingException(
                  "Decoder not found:" + type + " , DataType is :" + dataType);
      }
    } else if (type == TSEncoding.REGULAR) {
      switch (dataType) {
        case INT32:
          return new RegularDataDecoder.IntRegularDecoder();
        case INT64:
          return new RegularDataDecoder.LongRegularDecoder();
        default:
          throw new TsFileDecodingException(
              String.format(ERROR_MSG, type, dataType));
      }
    } else {
      throw new TsFileDecodingException(
          String.format(ERROR_MSG, type, dataType));
    }
  }

  public int readInt(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readInt is not supproted by Decoder");
  }

  public boolean readBoolean(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBoolean is not supproted by Decoder");
  }

  public short readShort(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readShort is not supproted by Decoder");
  }

  public long readLong(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readLong is not supproted by Decoder");
  }

  public float readFloat(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readFloat is not supproted by Decoder");
  }

  public double readDouble(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readDouble is not supproted by Decoder");
  }

  public Binary readBinary(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBinary is not supproted by Decoder");
  }

  public BigDecimal readBigDecimal(ByteBuffer buffer) {
    throw new TsFileDecodingException("Method readBigDecimal is not supproted by Decoder");
  }

  public abstract boolean hasNext(ByteBuffer buffer) throws IOException;

  public abstract void reset();

}
