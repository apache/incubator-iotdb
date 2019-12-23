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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.ICompressor;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * @see IChunkWriter IChunkWriter
 */
public class ChunkWriterImpl implements IChunkWriter {

    private static final Logger logger = LoggerFactory.getLogger(ChunkWriterImpl.class);

    private MeasurementSchema measurementSchema;

    /**
     * help to encode data of this series.
     */
    //private final ChunkBuffer chunkBuffer;
    private ICompressor compressor;

    /**
     * all pages of this column.
     */
    private PublicBAOS pageBuffer;

    private long chunkPointCount;

    private int numOfPages;
    /**
     * value writer to encode data.
     */
    private PageWriter pageWriter;

    /**
     * page size threshold.
     */
    private final long pageSizeThreshold;

    private final int maxNumberOfPointsInPage;

    // initial value for this.valueCountInOnePageForNextCheck
    private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1500;

    /**
     * value count in a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
     */
    private int valueCountInOnePageForNextCheck;

    /**
     * statistic on a stage. It will be reset after calling {@code writeAllPagesOfSeriesToTsFile()}
     */
    private Statistics<?> chunkStatistics;
    // time of the latest written time value pair

    /**
     * statistic on a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
     */
    //private Statistics<?> pageStatistics;

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

        // init statistics for this series and page
        this.chunkStatistics = Statistics.getStatsByType(measurementSchema.getType());

        this.pageWriter = new PageWriter(measurementSchema);
        this.pageWriter.setTimeEncoder(measurementSchema.getTimeEncoder());
        this.pageWriter.setValueEncoder(measurementSchema.getValueEncoder());
    }

    @Override
    public void write(long time, long value) {
        pageWriter.write(time, value);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, int value) {
        pageWriter.write(time, value);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, boolean value) {
        pageWriter.write(time, value);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, float value) {
        pageWriter.write(time, value);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, double value) {
        pageWriter.write(time, value);
        checkPageSizeAndMayOpenANewPage();
    }


    @Override
    public void write(long time, Binary value) {
        pageWriter.write(time, value);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long[] timestamps, int[] values, int batchSize) {
        pageWriter.write(timestamps, values, batchSize);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long[] timestamps, long[] values, int batchSize) {
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
        pageWriter.write(timestamps, values, batchSize);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long[] timestamps, double[] values, int batchSize) {
        pageWriter.write(timestamps, values, batchSize);
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long[] timestamps, Binary[] values, int batchSize) {
        pageWriter.write(timestamps, values, batchSize);
        checkPageSizeAndMayOpenANewPage();
    }

    /**
     * check occupied memory size, if it exceeds the PageSize threshold, flush them to given
     * OutputStream.
     */
    private void checkPageSizeAndMayOpenANewPage() {
        if (pageWriter.getPointNumber() == maxNumberOfPointsInPage) {
            logger.debug("current line count reaches the upper bound, write page {}", measurementSchema);
            writePage();
        } else if (pageWriter.getPointNumber()
                >= valueCountInOnePageForNextCheck) { // need to check memory size
            // not checking the memory used for every value
            long currentPageSize = pageWriter.estimateMaxMemSize();
            if (currentPageSize > pageSizeThreshold) { // memory size exceeds threshold
                // we will write the current page
                logger.debug(
                        "enough size, write page {}, pageSizeThreshold:{}, currentPateSize:{}, valueCountInOnePage:{}",
                        measurementSchema.getMeasurementId(), pageSizeThreshold, currentPageSize,
                        pageWriter.getPointNumber());
                writePage();
                valueCountInOnePageForNextCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
            } else {
                // reset the valueCountInOnePageForNextCheck for the next page
                valueCountInOnePageForNextCheck = (int) (((float) pageSizeThreshold / currentPageSize)
                        * pageWriter.getPointNumber());
            }
        }
    }

    private void writePage() {
        try {
            pageWriter.writePageHeaderAndDataIntoBuff(pageBuffer);

            // update statistics of this chunk
            Statistics statistics=pageWriter.getStatistics();
            numOfPages++;
            chunkPointCount += pageWriter.getPointNumber();
            this.chunkStatistics.mergeStatistics(statistics);
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
        writeAllPagesOfChunkToTsFile(tsfileWriter, chunkStatistics);
        this.reset();
        // reset series_statistics
        this.chunkStatistics = Statistics.getStatsByType(measurementSchema.getType());
    }

    @Override
    public long estimateMaxSeriesMemSize() {
        return pageWriter.estimateMaxMemSize() + this.estimateMaxPageMemSize();
    }

    @Override
    public long getCurrentChunkSize() {
        // return the serialized size of the chunk header + all pages
        return ChunkHeader.getSerializedSize(measurementSchema.getMeasurementId()) + this
                .getCurrentDataSize();
    }

    @Override
    public void sealCurrentPage() {
        if (pageWriter.getPointNumber() > 0) {
            writePage();
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
     * NOTE: for upgrading 0.8.0 to 0.9.0
     */
    public void writePageHeaderAndDataIntoBuff(ByteBuffer data, PageHeader header)
            throws PageException {
        numOfPages++;

        // write the page header to pageBuffer
        try {
            logger.debug("start to flush a page header into buffer, buffer position {} ", pageBuffer.size());
            header.serializeTo(pageBuffer);
            logger.debug("finish to flush a page header {} of {} into buffer, buffer position {} ", header,
                    measurementSchema.getMeasurementId(), pageBuffer.size());

        } catch (IOException e) {
            throw new PageException(
                    "IO Exception in writeDataPageHeader,ignore this page", e);
        }

        // update data point num
        this.chunkPointCount += header.getNumOfValues();

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
        if (statistics.getCount() == 0){
            return;
        }
        if (chunkPointCount == 0) {
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
     * reset exist data in page for next stage.
     */
    public void reset() {
        pageBuffer.reset();
        chunkPointCount = 0;
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


    /**
     * get current data size.
     *
     * @return current data size that the writer has serialized.
     */
    public long getCurrentDataSize() {
        return pageBuffer.size();
    }

    @Override
    public long getPtNum() {
        return chunkPointCount + pageWriter.getPointNumber();
    }
}
