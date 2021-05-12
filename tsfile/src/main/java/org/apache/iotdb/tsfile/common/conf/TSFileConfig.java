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
package org.apache.iotdb.tsfile.common.conf;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import java.io.Serializable;
import java.nio.charset.Charset;

/** TSFileConfig is a configure class. Every variables is public and has default value. */
public class TSFileConfig implements Serializable {

  /** encoding configuration */
  public static final int RLE_MIN_REPEATED_NUM = 8;

  public static final int RLE_MAX_REPEATED_NUM = 0x7FFF;
  public static final int RLE_MAX_BIT_PACKED_NUM = 63;

  public static final int FLOAT_VALUE_LENGTH = 6;
  public static final int DOUBLE_VALUE_LENGTH = 7;

  public static final int VALUE_BITS_LENGTH_32BIT = 32;
  public static final int LEADING_ZERO_BITS_LENGTH_32BIT = 5;
  public static final int MEANINGFUL_XOR_BITS_LENGTH_32BIT = 5;

  public static final int VALUE_BITS_LENGTH_64BIT = 64;
  public static final int LEADING_ZERO_BITS_LENGTH_64BIT = 6;
  public static final int MEANINGFUL_XOR_BITS_LENGTH_64BIT = 6;

  public static final int GORILLA_ENCODING_ENDING_INTEGER = Integer.MIN_VALUE;
  public static final long GORILLA_ENCODING_ENDING_LONG = Long.MIN_VALUE;
  public static final float GORILLA_ENCODING_ENDING_FLOAT = Float.NaN;
  public static final double GORILLA_ENCODING_ENDING_DOUBLE = Double.NaN;

  /** String encoder with UTF-8 encodes a character to at most 4 bytes. */
  public static final int BYTE_SIZE_PER_CHAR = 4;

  public static final String STRING_ENCODING = "UTF-8";
  public static final Charset STRING_CHARSET = Charset.forName(STRING_ENCODING);
  public static final String CONFIG_FILE_NAME = "iotdb-engine.properties";
  public static final String MAGIC_STRING = "TsFile";
  public static final String VERSION_NUMBER_V2 = "000002";
  public static final String VERSION_NUMBER_V1 = "000001";
  /** version number is changed to use 1 byte to represent since version 3 */
  public static final byte VERSION_NUMBER = 0x03;

  /** Bloom filter constrain */
  public static final double MIN_BLOOM_FILTER_ERROR_RATE = 0.01;

  public static final double MAX_BLOOM_FILTER_ERROR_RATE = 0.1;

  /** The primitive array capacity threshold. */
  public static final int ARRAY_CAPACITY_THRESHOLD = 1000;
  /** Memory size threshold for flushing to disk, default value is 128MB. */
  private int groupSizeInByte = 128 * 1024 * 1024;
  /** The memory size for each series writer to pack page, default value is 64KB. */
  private int pageSizeInByte = 64 * 1024;
  /** The maximum number of data points in a page, default value is 1024 * 1024. */
  private int maxNumberOfPointsInPage = 1024 * 1024;
  /** The maximum degree of a metadataIndex node, default value is 256 */
  private int maxDegreeOfIndexNode = 256;
  /** Data type for input timestamp, TsFile supports INT32 or INT64. */
  private String timeSeriesDataType = "INT64";
  /** Max length limitation of input string. */
  private int maxStringLength = 128;
  /** Floating-point precision. */
  private int floatPrecision = 2;
  /**
   * Encoder of time column, TsFile supports TS_2DIFF, PLAIN and RLE(run-length encoding) Default
   * value is TS_2DIFF.
   */
  private String timeEncoding = "TS_2DIFF";
  /**
   * Encoder of value series. default value is PLAIN. For int, long data type, TsFile also supports
   * TS_2DIFF, REGULAR, GORILLA and RLE(run-length encoding). For float, double data type, TsFile
   * also supports TS_2DIFF, RLE(run-length encoding) and GORILLA. For text data type, TsFile only
   * supports PLAIN.
   */
  private String valueEncoder = "PLAIN";
  /** Default bit width of RLE encoding is 8. */
  private int rleBitWidth = 8;
  /** Default block size of two-diff. delta encoding is 128 */
  private int deltaBlockSize = 128;
  /** Default frequency type is SINGLE_FREQ. */
  private String freqType = "SINGLE_FREQ";
  /** Default PLA max error is 100. */
  private double plaMaxError = 100;
  /** Default SDT max error is 100. */
  private double sdtMaxError = 100;
  /** Default DFT satisfy rate is 0.1 */
  private double dftSatisfyRate = 0.1;
  /** Data compression method, TsFile supports UNCOMPRESSED, SNAPPY or LZ4. */
  private CompressionType compressor = CompressionType.SNAPPY;
  /** Line count threshold for checking page memory occupied size. */
  private int pageCheckSizeThreshold = 100;
  /** Default endian value is BIG_ENDIAN. */
  private String endian = "BIG_ENDIAN";
  /** Default storage is in local file system */
  private FSType[] tsFileStorageFs = {FSType.LOCAL};
  /** Default core-site.xml file path is /etc/hadoop/conf/core-site.xml */
  private String coreSitePath = "/etc/hadoop/conf/core-site.xml";
  /** Default hdfs-site.xml file path is /etc/hadoop/conf/hdfs-site.xml */
  private String hdfsSitePath = "/etc/hadoop/conf/hdfs-site.xml";
  /** Default hdfs ip is localhost */
  private String hdfsIp = "localhost";
  /** Default hdfs port is 9000 */
  private String hdfsPort = "9000";
  /** Default DFS NameServices is hdfsnamespace */
  private String dfsNameServices = "hdfsnamespace";
  /** Default DFS HA name nodes are nn1 and nn2 */
  private String dfsHaNamenodes = "nn1,nn2";
  /** Default DFS HA automatic failover is enabled */
  private boolean dfsHaAutomaticFailoverEnabled = true;
  /**
   * Default DFS client failover proxy provider is
   * "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   */
  private String dfsClientFailoverProxyProvider =
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";
  /** whether use kerberos to authenticate hdfs */
  private boolean useKerberos = false;
  /** full path of kerberos keytab file */
  private String kerberosKeytabFilePath = "/path";
  /** kerberos pricipal */
  private String kerberosPrincipal = "principal";
  /** The acceptable error rate of bloom filter */
  private double bloomFilterErrorRate = 0.05;
  /** The amount of data iterate each time */
  private int batchSize = 1000;

  public TSFileConfig() {}

  public boolean isFSSupported(FSType fsType) {
    for (FSType fs : tsFileStorageFs) {
      if (fsType.equals(fs)) {
        return true;
      }
    }
    return false;
  }

  public int getGroupSizeInByte() {
    return groupSizeInByte;
  }

  public void setGroupSizeInByte(int groupSizeInByte) {
    this.groupSizeInByte = groupSizeInByte;
  }

  public int getPageSizeInByte() {
    return pageSizeInByte;
  }

  public void setPageSizeInByte(int pageSizeInByte) {
    this.pageSizeInByte = pageSizeInByte;
  }

  public int getMaxNumberOfPointsInPage() {
    return maxNumberOfPointsInPage;
  }

  public void setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    this.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
  }

  public int getMaxDegreeOfIndexNode() {
    return maxDegreeOfIndexNode;
  }

  public void setMaxDegreeOfIndexNode(int maxDegreeOfIndexNode) {
    this.maxDegreeOfIndexNode = maxDegreeOfIndexNode;
  }

  public String getTimeSeriesDataType() {
    return timeSeriesDataType;
  }

  // TS_2DIFF configuration

  public void setTimeSeriesDataType(String timeSeriesDataType) {
    this.timeSeriesDataType = timeSeriesDataType;
  }

  public int getMaxStringLength() {
    return maxStringLength;
  }

  // Freq encoder configuration

  public void setMaxStringLength(int maxStringLength) {
    this.maxStringLength = maxStringLength;
  }

  public int getFloatPrecision() {
    return floatPrecision;
  }

  public void setFloatPrecision(int floatPrecision) {
    this.floatPrecision = floatPrecision;
  }

  public String getTimeEncoder() {
    return timeEncoding;
  }

  // Compression configuration

  public void setTimeEncoder(String timeEncoder) {
    this.timeEncoding = timeEncoder;
  }

  // Don't change the following configuration

  public String getValueEncoder() {
    return valueEncoder;
  }

  public void setValueEncoder(String valueEncoder) {
    this.valueEncoder = valueEncoder;
  }

  public int getRleBitWidth() {
    return rleBitWidth;
  }

  public void setRleBitWidth(int rleBitWidth) {
    this.rleBitWidth = rleBitWidth;
  }

  public int getDeltaBlockSize() {
    return deltaBlockSize;
  }

  public void setDeltaBlockSize(int deltaBlockSize) {
    this.deltaBlockSize = deltaBlockSize;
  }

  public String getFreqType() {
    return freqType;
  }

  public void setFreqType(String freqType) {
    this.freqType = freqType;
  }

  public double getPlaMaxError() {
    return plaMaxError;
  }

  public void setPlaMaxError(double plaMaxError) {
    this.plaMaxError = plaMaxError;
  }

  public double getSdtMaxError() {
    return sdtMaxError;
  }

  public void setSdtMaxError(double sdtMaxError) {
    this.sdtMaxError = sdtMaxError;
  }

  public double getDftSatisfyRate() {
    return dftSatisfyRate;
  }

  public void setDftSatisfyRate(double dftSatisfyRate) {
    this.dftSatisfyRate = dftSatisfyRate;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public void setCompressor(String compressor) {
    this.compressor = CompressionType.valueOf(compressor);
  }

  public int getPageCheckSizeThreshold() {
    return pageCheckSizeThreshold;
  }

  public void setPageCheckSizeThreshold(int pageCheckSizeThreshold) {
    this.pageCheckSizeThreshold = pageCheckSizeThreshold;
  }

  public String getEndian() {
    return endian;
  }

  public void setEndian(String endian) {
    this.endian = endian;
  }

  public boolean isUseKerberos() {
    return useKerberos;
  }

  public void setUseKerberos(boolean useKerberos) {
    this.useKerberos = useKerberos;
  }

  public String getKerberosKeytabFilePath() {
    return kerberosKeytabFilePath;
  }

  public void setKerberosKeytabFilePath(String kerberosKeytabFilePath) {
    this.kerberosKeytabFilePath = kerberosKeytabFilePath;
  }

  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  public void setKerberosPrincipal(String kerberosPrincipal) {
    this.kerberosPrincipal = kerberosPrincipal;
  }

  public double getBloomFilterErrorRate() {
    return bloomFilterErrorRate;
  }

  public void setBloomFilterErrorRate(double bloomFilterErrorRate) {
    this.bloomFilterErrorRate = bloomFilterErrorRate;
  }

  public FSType[] getTSFileStorageFs() {
    return this.tsFileStorageFs;
  }

  public void setTSFileStorageFs(FSType[] fileStorageFs) {
    this.tsFileStorageFs = fileStorageFs;
  }

  public void setTSFileStorageFs(String[] fileStorageFs) {
    FSType[] fsTypes = new FSType[fileStorageFs.length];
    for (int i = 0; i < fileStorageFs.length; ++i) {
      fsTypes[i] = FSType.valueOf(fileStorageFs[i]);
    }
    this.setTSFileStorageFs(fsTypes);
  }

  public String getCoreSitePath() {
    return coreSitePath;
  }

  public void setCoreSitePath(String coreSitePath) {
    this.coreSitePath = coreSitePath;
  }

  public String getHdfsSitePath() {
    return hdfsSitePath;
  }

  public void setHdfsSitePath(String hdfsSitePath) {
    this.hdfsSitePath = hdfsSitePath;
  }

  public String[] getHdfsIp() {
    return hdfsIp.split(",");
  }

  public void setHdfsIp(String[] hdfsIp) {
    this.hdfsIp = String.join(",", hdfsIp);
  }

  public String getHdfsPort() {
    return this.hdfsPort;
  }

  public void setHdfsPort(String hdfsPort) {
    this.hdfsPort = hdfsPort;
  }

  public String getDfsNameServices() {
    return dfsNameServices;
  }

  public void setDfsNameServices(String dfsNameServices) {
    this.dfsNameServices = dfsNameServices;
  }

  public String[] getDfsHaNamenodes() {
    return dfsHaNamenodes.split(",");
  }

  public void setDfsHaNamenodes(String[] dfsHaNamenodes) {
    this.dfsHaNamenodes = String.join(",", dfsHaNamenodes);
  }

  public boolean isDfsHaAutomaticFailoverEnabled() {
    return dfsHaAutomaticFailoverEnabled;
  }

  public void setDfsHaAutomaticFailoverEnabled(boolean dfsHaAutomaticFailoverEnabled) {
    this.dfsHaAutomaticFailoverEnabled = dfsHaAutomaticFailoverEnabled;
  }

  public String getDfsClientFailoverProxyProvider() {
    return dfsClientFailoverProxyProvider;
  }

  public void setDfsClientFailoverProxyProvider(String dfsClientFailoverProxyProvider) {
    this.dfsClientFailoverProxyProvider = dfsClientFailoverProxyProvider;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }
}
