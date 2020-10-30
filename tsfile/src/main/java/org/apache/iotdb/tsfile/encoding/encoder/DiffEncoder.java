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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * <p>
 * DiffEncoder is a encoder for compressing data in type of INT32(integer) and
 * INT64(long). It is based in delta-encoding arithmetic. We adapt a hypothesis
 * that contiguous data points have similar values.Thus the difference value of
 * two adjacent points is smaller than those two point values. One integer in
 * java takes 32-bits. If a positive number is less than 2^m, the bits of this
 * integer which index from m to 31 are all 0. Given an array which length is n,
 * if all values in input data array are all positive and less than 2^m, we need
 * actually m*n, but not 32*n bits to store the array.
 * </p>
 * <p>
 * DiffEncoder calculates difference between two adjacent. Then it saves the delta
 * values. Then it counts the longest bit length {@code m} it takes for each
 * delta value, which means the bit length that maximum delta value takes. Only
 * the low {@code m} bits are saved into result byte array for all delta values.
 * </p>
 */
public abstract class DiffEncoder extends Encoder {
  protected static final int BLOCK_DEFAULT_SIZE = 128;
  private static final Logger logger = LoggerFactory.getLogger(DiffEncoder.class);
  protected ByteArrayOutputStream out;
  protected int blockSize;

  protected byte[] encodingBlockBuffer;

  protected int writeIndex = -1;
  protected int writeWidth = 0;

  /**
   * constructor of DiffEncoder.
   *
   * @param size - the number how many numbers to be packed into a block.
   */
  public DiffEncoder(int size) {
    super(TSEncoding.DIFF);
    blockSize = size;
  }

  protected abstract void writeHeader() throws IOException;

  protected abstract void writeValueToBytes(int i);

  protected abstract void reset();

  protected abstract int calculateBitWidthsForDeltaBlockBuffer();

  /**
   * write all data into {@code encodingBlockBuffer}.
   */
  private void writeDataWithMinWidth() {
    for (int i = 0; i < writeIndex; i++) {
      writeValueToBytes(i);
    }
    int encodingLength = (int) Math.ceil((double) (writeIndex * writeWidth) / 8.0);
    out.write(encodingBlockBuffer, 0, encodingLength);
  }

  private void writeHeaderToBytes() throws IOException {
    ReadWriteIOUtils.write(writeIndex, out);
    ReadWriteIOUtils.write(writeWidth, out);
    writeHeader();
  }


  /**
   * Write the data to byteArrayOutPutStream.
   */
  private void flushBlockBuffer(ByteArrayOutputStream out) throws IOException {
    if (writeIndex == -1) {
      return;
    }
    this.out = out;
    writeWidth = calculateBitWidthsForDeltaBlockBuffer();
    writeHeaderToBytes();
    writeDataWithMinWidth();

    reset();
    writeIndex = -1;
  }

  /**
   * calling this method to flush all values which haven't encoded to result byte
   * array.
   * call flushBlockBuffer Funciton.
   */
  @Override
  public void flush(ByteArrayOutputStream out) {
    try {
      flushBlockBuffer(out);
    } catch (IOException e) {
      logger.error("flush data to stream failed!", e);
    }
  }

  public static class IntDeltaEncoder extends DiffEncoder {

    private int[] deltaBlockBuffer;
    private int firstValue;
    private int previousValue;

    public IntDeltaEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of IntDeltaEncoder which is a sub-class of DiffEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public IntDeltaEncoder(int size) {
      super(size);// choose TS_2DIFF + initialize deltaSize=size
      deltaBlockBuffer = new int[this.blockSize]; //temporary save the data to be encode.
      encodingBlockBuffer = new byte[blockSize * 4]; //Save encoded data.
      reset();//Initialization
    }

    @Override
    //calculate max BitWidth.
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(deltaBlockBuffer[i]));
      }
      return width;
    }

    private void calcDelta(Integer value) {
      Integer delta = value - previousValue;// calculate delta
      deltaBlockBuffer[writeIndex++] = delta;
    }

    /**
     * input a integer.
     *
     * @param value value to encode
     * @param out   the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(int value, ByteArrayOutputStream out) {
      //writeIndex's initial value=-1, to identify the first input here.
      if (writeIndex == -1) {
        writeIndex++;
        firstValue = value;
        previousValue = firstValue;
        return;
      }
      calcDelta(value);
      previousValue = value;
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    @Override
    protected void reset() {
      firstValue = 0;
      previousValue = 0;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        deltaBlockBuffer[i] = 0;
      }
    }

    //cal width, size - leadingZeros
    private int getValueWidth(int v) {
      return 32 - Integer.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes(int i) {
      BytesUtils.intToBytes(deltaBlockBuffer[i], encodingBlockBuffer, writeWidth * i, writeWidth);
    }

    @Override
    protected void writeHeader() throws IOException {
      ReadWriteIOUtils.write(firstValue, out);
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return 4;
    }

    @Override
    public long getMaxByteSize() {
      // The meaning of 24 is: index(4)+width(4)+minDeltaBase(4)+firstValue(4)
      return (long) 24 + writeIndex * 4;
    }
  }

  public static class LongDeltaEncoder extends DiffEncoder {

    private long[] deltaBlockBuffer;
    private long firstValue;
    private long previousValue;

    public LongDeltaEncoder() {
      this(BLOCK_DEFAULT_SIZE);
    }

    /**
     * constructor of LongDeltaEncoder which is a sub-class of DiffEncoder.
     *
     * @param size - the number how many numbers to be packed into a block.
     */
    public LongDeltaEncoder(int size) {
      super(size);
      deltaBlockBuffer = new long[this.blockSize];
      encodingBlockBuffer = new byte[blockSize * 8];
      reset();
    }

    private void calcDelta(Long value) {
      Long delta = value - previousValue;// calculate delta
      deltaBlockBuffer[writeIndex++] = delta;
    }

    @Override
    protected void reset() {
      firstValue = 0L;
      previousValue = 0L;
      for (int i = 0; i < blockSize; i++) {
        encodingBlockBuffer[i] = 0;
        deltaBlockBuffer[i] = 0L;
      }
    }

    private int getValueWidth(Long v) {
      return 64 - Long.numberOfLeadingZeros(v);
    }

    @Override
    protected void writeValueToBytes(int i) {
      BytesUtils.longToBytes(deltaBlockBuffer[i], encodingBlockBuffer, writeWidth * i, writeWidth);
    }

    @Override
    protected void writeHeader() throws IOException {
      out.write(BytesUtils.longToBytes(firstValue));
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
      encodeValue(value, out);
    }

    @Override
    public int getOneItemMaxSize() {
      return 8;
    }

    @Override
    public long getMaxByteSize() {
      return (long) 24 + writeIndex * 8;
    }

    /**
     * input a integer or long value.
     *
     * @param value value to encode
     * @param out   - the ByteArrayOutputStream which data encode into
     */
    public void encodeValue(long value, ByteArrayOutputStream out) {
      if (writeIndex == -1) {
        writeIndex++;
        firstValue = value;
        previousValue = firstValue;
        return;
      }
      calcDelta(value);
      previousValue = value;
      if (writeIndex == blockSize) {
        flush(out);
      }
    }

    @Override
    protected int calculateBitWidthsForDeltaBlockBuffer() {
      int width = 0;
      for (int i = 0; i < writeIndex; i++) {
        width = Math.max(width, getValueWidth(deltaBlockBuffer[i]));
      }
      return width;
    }
  }
}
