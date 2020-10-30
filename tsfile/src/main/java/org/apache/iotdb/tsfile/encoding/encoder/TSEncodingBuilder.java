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

package org.apache.iotdb.tsfile.encoding.encoder;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

/**
 * Each subclass of TSEncodingBuilder responds a enumerate value in {@linkplain TSEncoding
 * TSEncoding}, which stores several configuration related to responding encoding type to generate
 * {@linkplain Encoder Encoder} instance.<br> Each TSEncoding has a responding TSEncodingBuilder.
 * The design referring to visit pattern provides same outer interface for different TSEncodings and
 * gets rid of the duplicate switch-case code.
 */
public abstract class TSEncodingBuilder {

  private static final Logger logger = LoggerFactory.getLogger(TSEncodingBuilder.class);
  protected final TSFileConfig conf;

  public TSEncodingBuilder() {
    this.conf = TSFileDescriptor.getInstance().getConfig();
  }

  /**
   * return responding TSEncodingBuilder from a TSEncoding.
   *
   * @param type - given encoding type
   * @return - responding TSEncodingBuilder
   */
  public static TSEncodingBuilder getEncodingBuilder(TSEncoding type) {
    switch (type) {
      case PLAIN:
        return new PLAIN();
      case RLE:
        return new RLE();
      case TS_2DIFF:
        return new TS_2DIFF();
      case DIFF:
        return new DIFF();
      case GORILLA:
        return new GORILLA();
      case REGULAR:
        return new REGULAR();
      default:
        throw new UnsupportedOperationException(type.toString());
    }
  }

  /**
   * return a thread safe series's encoder with different types and parameters according to its
   * measurement id and data type.
   *
   * @param type - given data type
   * @return - return a {@linkplain Encoder Encoder}
   */
  public abstract Encoder getEncoder(TSDataType type);

  /**
   * for TSEncoding, JSON is a kind of type for initialization. {@code InitFromJsonObject} gets
   * values from JSON object which will be used latter.<br> if this type has extra parameters to
   * construct, override it.
   *
   * @param props - properties of encoding
   */
  public abstract void initFromProps(Map<String, String> props);

  @Override
  public String toString() {
    return "";
  }

  /**
   * for all TSDataType.
   */
  public static class PLAIN extends TSEncodingBuilder {

    private int maxStringLength = TSFileDescriptor.getInstance().getConfig().getMaxStringLength();

    @Override
    public Encoder getEncoder(TSDataType type) {
      return new PlainEncoder(EndianType.BIG_ENDIAN, type, maxStringLength);
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // set max error from initialized map or default value if not set
      if (props == null || !props.containsKey(Encoder.MAX_STRING_LENGTH)) {
        maxStringLength = TSFileDescriptor.getInstance().getConfig().getMaxStringLength();
      } else {
        maxStringLength = Integer.valueOf(props.get(Encoder.MAX_STRING_LENGTH));
        if (maxStringLength < 0) {
          maxStringLength = TSFileDescriptor.getInstance().getConfig().getMaxStringLength();
          logger.warn(
              "cannot set max string length to negative value, replaced with default value:{}",
              maxStringLength);
        }
      }
    }
  }

  /**
   * for ENUMS, INT32, BOOLEAN, INT64, FLOAT, DOUBLE.
   */
  public static class RLE extends TSEncodingBuilder {

    private int maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
        case BOOLEAN:
          return new IntRleEncoder(EndianType.BIG_ENDIAN);
        case INT64:
          return new LongRleEncoder(EndianType.BIG_ENDIAN);
        case FLOAT:
        case DOUBLE:
          return new FloatEncoder(TSEncoding.RLE, type, maxPointNumber);
        default:
          throw new UnSupportedDataTypeException("RLE doesn't support data type: " + type);
      }
    }

    /**
     * RLE could specify <b>max_point_number</b> in given JSON Object, which means the maximum
     * decimal digits for float or double data.
     */
    @Override
    public void initFromProps(Map<String, String> props) {
      // set max error from initialized map or default value if not set
      if (props == null || !props.containsKey(Encoder.MAX_POINT_NUMBER)) {
        maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      } else {
        maxPointNumber = Integer.valueOf(props.get(Encoder.MAX_POINT_NUMBER));
        if (maxPointNumber < 0) {
          maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
          logger
              .warn("cannot set max point number to negative value, replaced with default value:{}",
                  maxPointNumber);
        }
      }
    }

    @Override
    public String toString() {
      return JsonFormatConstant.MAX_POINT_NUMBER + ":" + maxPointNumber;
    }
  }

  /**
   * for INT32, INT64, FLOAT, DOUBLE.
   */
  public static class TS_2DIFF extends TSEncodingBuilder {

    private int maxPointNumber = 0;

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
          return new DeltaBinaryEncoder.IntDeltaEncoder();
        case INT64:
          return new DeltaBinaryEncoder.LongDeltaEncoder();
        case FLOAT:
        case DOUBLE:
          return new FloatEncoder(TSEncoding.TS_2DIFF, type, maxPointNumber);
        default:
          throw new UnSupportedDataTypeException("TS_2DIFF doesn't support data type: " + type);
      }
    }

    @Override
    /**
     * TS_2DIFF could specify <b>max_point_number</b> in given JSON Object, which
     * means the maximum decimal digits for float or double data.
     */
    public void initFromProps(Map<String, String> props) {
      // set max error from initialized map or default value if not set
      if (props == null || !props.containsKey(Encoder.MAX_POINT_NUMBER)) {
        maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      } else {
        maxPointNumber = Integer.valueOf(props.get(Encoder.MAX_POINT_NUMBER));
        if (maxPointNumber < 0) {
          maxPointNumber = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
          logger
              .warn("cannot set max point number to negative value, replaced with default value:{}",
                  maxPointNumber);
        }
      }
    }

    @Override
    public String toString() {
      return JsonFormatConstant.MAX_POINT_NUMBER + ":" + maxPointNumber;
    }

  }

  /**
   * for ENUMS.
   */
  public static class GORILLA extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case FLOAT:
          return new SinglePrecisionEncoder();
        case DOUBLE:
          return new DoublePrecisionEncoder();
        default:
          throw new UnSupportedDataTypeException("GORILLA doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // allowed do nothing
    }

  }

  /**
   * for INT32 and INT64.
   */
  public static class DIFF extends TSEncodingBuilder {
    @Override
    public Encoder getEncoder(TSDataType type){
      switch (type) {
        case INT32:
          return new DiffEncoder.IntDeltaEncoder();
        case INT64:
          return new DiffEncoder.LongDeltaEncoder();
        default:
          throw new UnSupportedDataTypeException("DIFF doesn't support data type: " + type);
      }
    }
    @Override
    public void initFromProps(Map<String, String> props) {
      // allowed do nothing
    }
  }

  /**
   * for INT32, INT64
   */
  public static class REGULAR extends TSEncodingBuilder {

    @Override
    public Encoder getEncoder(TSDataType type) {
      switch (type) {
        case INT32:
          return new RegularDataEncoder.IntRegularEncoder();
        case INT64:
          return new RegularDataEncoder.LongRegularEncoder();
        default:
          throw new UnSupportedDataTypeException("REGULAR doesn't support data type: " + type);
      }
    }

    @Override
    public void initFromProps(Map<String, String> props) {
      // allowed do nothing
    }
  }
}
