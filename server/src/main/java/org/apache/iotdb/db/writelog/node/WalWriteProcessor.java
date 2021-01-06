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
package org.apache.iotdb.db.writelog.node;

import java.nio.ByteBuffer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.writelog.io.ILogWriter;

public class WalWriteProcessor {

  private boolean isPrevious;

  private ILogWriter fileWriter;

  private ByteBuffer logBufferWorking = ByteBuffer
      .allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
  private ByteBuffer logBufferIdle = ByteBuffer
      .allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);

  private ByteBuffer logBufferFlushing;

  private final Object switchBufferCondition = new Object();

  private int bufferedLogNum = 0;

  public WalWriteProcessor(boolean isPrevious) {
    this.isPrevious = isPrevious;
  }

  public void setPrevious(boolean previous) {
    isPrevious = previous;
  }

  public boolean isPrevois() {
    return isPrevious;
  }

  public ByteBuffer getLogBufferWorking() {
    return logBufferWorking;
  }

  public void setLogBufferWorking(ByteBuffer logBufferWorking) {
    this.logBufferWorking = logBufferWorking;
  }

  public ByteBuffer getLogBufferIdle() {
    return logBufferIdle;
  }

  public void setLogBufferIdle(ByteBuffer logBufferIdle) {
    this.logBufferIdle = logBufferIdle;
  }

  public ByteBuffer getLogBufferFlushing() {
    return logBufferFlushing;
  }

  public void setLogBufferFlushing(ByteBuffer logBufferFlushing) {
    this.logBufferFlushing = logBufferFlushing;
  }

  public void incrementLogCnt(){
    bufferedLogNum++;
  }

  public Object getSwitchBufferCondition() {
    return switchBufferCondition;
  }

  public int getBufferedLogNum(){
    return bufferedLogNum;
  }

  public void setBufferedLogNum(int bufferedLogNum) {
    this.bufferedLogNum = bufferedLogNum;
  }

  public ILogWriter getFileWriter() {
    return fileWriter;
  }

  public void setFileWriter(ILogWriter fileWriter) {
    this.fileWriter = fileWriter;
  }
}
