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
package org.apache.iotdb.tsfile.write.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkWriterImpl implements IChunkWriter {

  private static final Logger logger = LoggerFactory.getLogger(ChunkWriterImpl.class);

  private MeasurementSchema measurementSchema;

  private ICompressor compressor;

  /**
   * all pages of this chunk.
   */
  private PublicBAOS pageBuffer;

  private int numOfPages;

  /**
   * write data into current page
   */
  private PageWriter pageWriter;

  /**
   * page size threshold.
   */
  private final long pageSizeThreshold;

  private final int maxNumberOfPointsInPage;

  /**
   * value count in current page.
   */
  private int valueCountInOnePageForNextCheck;

  // initial value for valueCountInOnePageForNextCheck
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

  /**
   * statistic of this chunk.
   */
  private Statistics<?> statistics;

  private boolean isSdtEncoding;

  private SDTEncoder sdtEncoder;

  /**
   * do not re-execute SDT compression when merging chunks
   */
  private boolean isMerging;

  /**
   * @param schema schema of this measurement
   */
  public ChunkWriterImpl(MeasurementSchema schema) {
    this.measurementSchema = schema;
    this.compressor = ICompressor.getCompressor(schema.getCompressor());
    this.pageBuffer = new PublicBAOS();

    this.pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxNumberOfPointsInPage = TSFileDescriptor.getInstance().getConfig()
        .getMaxNumberOfPointsInPage();
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

    // init statistics for this chunk and page
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());

    this.pageWriter = new PageWriter(measurementSchema);
    this.pageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
    this.pageWriter.setValueEncoder(measurementSchema.getValueEncoder());

    //check if the measurement schema uses SDT
    checkSdtEncoding();
  }

  public ChunkWriterImpl(MeasurementSchema schema, boolean isMerging) {
    this(schema);
    this.isMerging = isMerging;
  }

  private void checkSdtEncoding() {
    if (measurementSchema.getProps() != null && !isMerging) {
      if (measurementSchema.getProps().getOrDefault("loss", "").equals("sdt")) {
        isSdtEncoding = true;
        sdtEncoder = new SDTEncoder();
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey("compdev")) {
        try {
          sdtEncoder.setCompDeviation(Double.parseDouble(measurementSchema.getProps().get("compdev")));
        } catch (NumberFormatException e) {
          logger.error("meet error when formatting SDT compression deviation");
        }
        if (sdtEncoder.getCompDeviation() < 0) {
          logger
              .error("SDT compression deviation cannot be negative. SDT encoding is turned off.");
          isSdtEncoding = false;
        }
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey("compmin")) {
        try {
          sdtEncoder.setCompMin(Double.parseDouble(measurementSchema.getProps().get("compmin")));
        } catch (NumberFormatException e) {
          logger.error("meet error when formatting SDT compression minimum");
        }
      }

      if (isSdtEncoding && measurementSchema.getProps().containsKey("compmax")) {
        try {
          sdtEncoder.setCompMax(Double.parseDouble(measurementSchema.getProps().get("compmax")));
        } catch (NumberFormatException e) {
          logger.error("meet error when formatting SDT compression maximum");
        }
      }

      if (isSdtEncoding && sdtEncoder.getCompMax() <= sdtEncoder.getCompMin()) {
        logger
            .error("SDT compression maximum needs to be greater than compression minimum. SDT encoding is turned off");
        isSdtEncoding = false;
      }
    }
  }

  @Override
  public void write(long time, long value) {
    if (!isSdtEncoding || sdtEncoder.encodeLong(time, value)) {
      if (isSdtEncoding) {
        //store last read time and value
        time = sdtEncoder.getTime();
        value = sdtEncoder.getLongValue();
      }
      pageWriter.write(time, value);
      checkPageSizeAndMayOpenANewPage();
    }
  }

  @Override
  public void write(long time, int value) {
    if (!isSdtEncoding || sdtEncoder.encodeInt(time, value)) {
      if (isSdtEncoding) {
        time = sdtEncoder.getTime();
        value = sdtEncoder.getIntValue();
      }
      pageWriter.write(time, value);
      checkPageSizeAndMayOpenANewPage();
    }
  }

  @Override
  public void write(long time, boolean value) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long time, float value) {
    if (!isSdtEncoding || sdtEncoder.encodeFloat(time, value)) {
      if (isSdtEncoding) {
        time = sdtEncoder.getTime();
        value = sdtEncoder.getFloatValue();
      }
      pageWriter.write(time, value);
      checkPageSizeAndMayOpenANewPage();
    }
  }

  @Override
  public void write(long time, double value) {
    if (!isSdtEncoding || sdtEncoder.encodeDouble(time, value)) {
      if (isSdtEncoding) {
        time = sdtEncoder.getTime();
        value = sdtEncoder.getDoubleValue();
      }
      pageWriter.write(time, value);
      checkPageSizeAndMayOpenANewPage();
    }
  }

  @Override
  public void write(long time, Binary value) {
    pageWriter.write(time, value);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, int[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, long[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, boolean[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, float[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, double[] values, int batchSize) {
    if (isSdtEncoding) {
      batchSize = sdtEncoder.encode(timestamps, values, batchSize);
    }
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  @Override
  public void write(long[] timestamps, Binary[] values, int batchSize) {
    pageWriter.write(timestamps, values, batchSize);
    checkPageSizeAndMayOpenANewPage();
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, construct a page and 
   * put it to pageBuffer
   */
  private void checkPageSizeAndMayOpenANewPage() {
    if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {
      logger.debug("current line count reaches the upper bound, write page {}", measurementSchema);
      writePageToPageBuffer();
    } else if (pageWriter.getPointNumber()
        >= valueCountInOnePageForNextCheck) { // need to check memory size
      // not checking the memory used for every value
      long currentPageSize = pageWriter.estimateMaxMemSize();
      if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
        // we will write the current page
        logger.trace(
            "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
            measurementSchema.getMeasurementId(), pageSizeThreshold, currentPageSize,
            pageWriter.getPointNumber());
        writePageToPageBuffer();
        valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
      } else {
        // reset the valueCountInOnePageForNextCheck for the next page
        valueCountInOnePageForNextCheck = (int) (((float) pageSizeThreshold / currentPageSize)
            * pageWriter.getPointNumber());
      }
    }
  }

  private void writePageToPageBuffer() {
    try {
      pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer);

      // update statistics of this chunk
      numOfPages++;
      this.statistics.mergeStatistics(pageWriter.getStatistics());
    } catch (IOException e) {
      logger.error("meet error in pageWriter.writePageHeaderAndDataIntoBuff,ignore this page:", e);
    } finally {
      // clear start time stamp for next initializing
      pageWriter.reset(measurementSchema);
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    sealCurrentPage();
    writeAllPagesOfChunkToTsFile(tsfileWriter, statistics);

    // reinit this chunk writer
    pageBuffer.reset();
    this.statistics = Statistics.getStatsByType(measurementSchema.getType());
  }

  @Override
  public long estimateMaxSeriesMemSize() {
    return pageWriter.estimateMaxMemSize() + this.estimateMaxPageMemSize();
  }

  @Override
  public long getCurrentChunkSize() {
    if (pageBuffer.size() == 0) {
      return 0;
    }
    // return the serialized size of the chunk header + all pages
    return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId()) + (long) pageBuffer.size();
  }

  @Override
  public void sealCurrentPage() {
    if (pageWriter.getPointNumber() > 0) {
      writePageToPageBuffer();
    }
  }

  @Override
  public int getNumOfPages() {
    return numOfPages;
  }

  @Override
  public TSDataType getDataType() {
    return measurementSchema.getType();
  }

  /**
   * write the page header and data into the PageWriter's output stream.
   *
   * @NOTE: for upgrading 0.9/v1 to 0.10/v2 TsFile
   */
  @Override
  public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
      throws PageException {
    numOfPages++;

    // write the page header to pageBuffer
    try {
      logger.debug("start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
      header.serializeTo(pageBuffer);
      logger.debug("finish to flush a page header {} of {} into buffer, buffer position {} ", header,
          measurementSchema.getMeasurementId(), pageBuffer.size());

      statistics.mergeStatistics(header.getStatistics());

    } catch (IOException e) {
      throw new PageException(
          "IO Exception in writeDataPageHeader,ignore this page", e);
    }

    // write page content to temp PBAOS
    try (WritableByteChannel channel = Channels.newChannel(pageBuffer)) {
      channel.write(data);
    } catch (IOException e) {
      throw new PageException(e);
    }
  }

  /**
   * write the page to specified IOWriter.
   *
   * @param writer     the specified IOWriter
   * @param statistics the chunk statistics
   * @throws IOException exception in IO
   */
  public void writeAllPagesOfChunkToTsFile(TsFileIOWriter writer, Statistics<?> statistics)
      throws IOException {
    if (statistics.getCount() == 0) {
      return;
    }

    // start to write this column chunk
    writer.startFlushChunk(measurementSchema, compressor.getType(), measurementSchema.getType(),
        measurementSchema.getEncodingType(), statistics, pageBuffer.size(), numOfPages);

    long dataOffset = writer.getPos();

    // write all pages of this column
    writer.writeBytesToStream(pageBuffer);

    long dataSize = writer.getPos() - dataOffset;
    if (dataSize != pageBuffer.size()) {
      throw new IOException(
          "Bytes written is inconsistent with the size of data: " + dataSize + " !="
              + " " + pageBuffer.size());
    }

    writer.endCurrentChunk();
  }

  /**
   * estimate max page memory size.
   *
   * @return the max possible allocated size currently
   */
  private long estimateMaxPageMemSize() {
    // return the sum of size of buffer and page max size
    return (long) (pageBuffer.size() +
        PageHeader.calculatePageHeaderSizeWithoutStatistics() +
        pageWriter.getStatistics().getSerializedSize());
  }

  public void setIsMerging(boolean isMerging) {
    this.isMerging = isMerging;
  }

  public boolean isMerging() {
    return isMerging;
  }
}
