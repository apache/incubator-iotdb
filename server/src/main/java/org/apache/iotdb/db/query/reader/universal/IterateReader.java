///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.iotdb.db.query.reader.universal;
//
//import java.io.IOException;
//import org.apache.iotdb.tsfile.file.header.PageHeader;
//import org.apache.iotdb.tsfile.read.common.BatchData;
//import org.apache.iotdb.tsfile.read.reader.IAggregateReader;
//import org.apache.iotdb.tsfile.read.reader.IChunkReader;
//
///**
// * This class is for sequential data sources.
// */
//public abstract class IterateReader implements IAggregateReader {
//
//  protected IChunkReader currentSeriesReader;
//  private boolean curReaderInitialized;
//  private int nextSeriesReaderIndex;
//  private int readerSize;
//
//
//  public IterateReader(int readerSize) {
//    this.curReaderInitialized = false;
//    this.nextSeriesReaderIndex = 0;
//    this.readerSize = readerSize;
//  }
//
//  @Override
//  public boolean hasNextBatch() throws IOException {
//
//    if (curReaderInitialized && currentSeriesReader.hasNextSatisfiedPage()) {
//      return true;
//    } else {
//      curReaderInitialized = false;
//    }
//
//    while (nextSeriesReaderIndex < readerSize) {
//      boolean isConstructed = constructNextReader(nextSeriesReaderIndex++);
//      if (isConstructed && currentSeriesReader.hasNextSatisfiedPage()) {
//        curReaderInitialized = true;
//        return true;
//      }
//    }
//    return false;
//  }
//
//  /**
//   * If the idx-th data source in order needs reading, construct <code>IBatchReader</code> for it,
//   * assign to <code>currentSeriesReader</code> and return true. Otherwise, return false.
//   *
//   * @param idx the index of the data source
//   * @return True if the reader is constructed; False if not.
//   */
//  protected abstract boolean constructNextReader(int idx) throws IOException;
//
//  @Override
//  public BatchData nextBatch() throws IOException {
//    return currentSeriesReader.nextPageData();
//  }
//
//  @Override
//  public void close() {
//    // file stream is managed in QueryResourceManager.
//  }
//
//  @Override
//  public PageHeader nextPageHeader() throws IOException {
//    return currentSeriesReader.nextPageHeader();
//  }
//
//  @Override
//  public void skipPageData() throws IOException {
//    currentSeriesReader.skipPageData();
//  }
//}
