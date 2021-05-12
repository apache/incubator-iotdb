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

package org.apache.iotdb.hadoop.fileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class HDFSFile extends File {

  private static final long serialVersionUID = -8419827359081949547L;
  private Path hdfsPath;
  private FileSystem fs;
  private static final Logger logger = LoggerFactory.getLogger(HDFSFile.class);
  private static final String UNSUPPORT_OPERATION = "Unsupported operation.";

  public HDFSFile(String pathname) {
    // invalid call of super constructor, just for grammar correct
    super(pathname);
    hdfsPath = new Path(pathname);
    setConfAndGetFS();
  }

  public HDFSFile(String parent, String child) {
    // invalid call of super constructor, just for grammar correct
    super(parent, child);
    hdfsPath = new Path(parent + HDFSPath.separator + child);
    setConfAndGetFS();
  }

  public HDFSFile(File parent, String child) {
    // invalid call of super constructor, just for grammar correct
    super(parent, child);
    hdfsPath = new Path(parent.getAbsolutePath() + HDFSPath.separator + child);
    setConfAndGetFS();
  }

  public HDFSFile(URI uri) {
    // invalid call of super constructor, just for grammar correct
    super(uri.getPath());
    hdfsPath = new Path(uri);
    setConfAndGetFS();
  }

  private void setConfAndGetFS() {
    Configuration conf = HDFSConfUtil.setConf(new Configuration());
    try {
      fs = hdfsPath.getFileSystem(conf);
    } catch (IOException e) {
      logger.error("Fail to get HDFS! ", e);
    }
  }

  @Override
  public String getAbsolutePath() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public String getPath() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public long length() {
    try {
      return fs.getFileStatus(hdfsPath).getLen();
    } catch (IOException e) {
      logger.error("Fail to get length of the file {}, ", hdfsPath.toUri(), e);
      return 0;
    }
  }

  @Override
  public boolean exists() {
    try {
      return fs.exists(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to check whether the file {} exists. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public File[] listFiles() {
    List<HDFSFile> files = new ArrayList<>();
    try {
      for (FileStatus fileStatus : fs.listStatus(hdfsPath)) {
        Path filePath = fileStatus.getPath();
        files.add(new HDFSFile(filePath.toUri().toString()));
      }
      return files.toArray(new HDFSFile[0]);
    } catch (IOException e) {
      logger.error("Fail to list files in {}. ", hdfsPath.toUri(), e);
      return null;
    }
  }

  @Override
  public File getParentFile() {
    return new HDFSFile(hdfsPath.getParent().toUri().toString());
  }

  @Override
  public boolean createNewFile() throws IOException {
    return fs.createNewFile(hdfsPath);
  }

  @Override
  public boolean delete() {
    try {
      return !fs.exists(hdfsPath) || fs.delete(hdfsPath, true);
    } catch (IOException e) {
      logger.error("Fail to delete file {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean mkdirs() {
    try {
      return !exists() && fs.mkdirs(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to create directory {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean isDirectory() {
    try {
      return exists() && fs.getFileStatus(hdfsPath).isDirectory();
    } catch (IOException e) {
      logger.error("Fail to judge whether {} is a directory. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public long getFreeSpace() {
    try {
      return fs.getStatus().getRemaining();
    } catch (IOException e) {
      logger.error("Fail to get free space of {}. ", hdfsPath.toUri(), e);
      return 0L;
    }
  }

  @Override
  public String getName() {
    return hdfsPath.getName();
  }

  @Override
  public String toString() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public int hashCode() {
    return hdfsPath.hashCode();
  }

  @Override
  public int compareTo(File pathname) {
    if (pathname instanceof HDFSFile) {
      return hdfsPath.toUri().toString().compareTo(pathname.getPath());
    } else {
      logger.error("File {} is not HDFS file. ", pathname.getPath());
      throw new IllegalArgumentException("Compare file is not HDFS file.");
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof HDFSFile && compareTo((HDFSFile) obj) == 0;
  }

  @Override
  public boolean renameTo(File dest) {
    try {
      return fs.rename(hdfsPath, new Path(dest.getAbsolutePath()));
    } catch (IOException e) {
      logger.error("Failed to rename file {} to {}. ", hdfsPath, dest.getName(), e);
      return false;
    }
  }

  @Override
  public File getAbsoluteFile() {
    return new HDFSFile(getAbsolutePath());
  }

  @Override
  public String getParent() {
    return hdfsPath.getParent().toUri().toString();
  }

  @Override
  public boolean isAbsolute() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public File[] listFiles(FileFilter filter) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public String getCanonicalPath() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public File getCanonicalFile() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public URL toURL() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public URI toURI() {
    return hdfsPath.toUri();
  }

  @Override
  public boolean canRead() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean canWrite() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean isFile() {
    try {
      return exists() && fs.getFileStatus(hdfsPath).isFile();
    } catch (IOException e) {
      logger.error("Fail to judge whether {} is a file. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean isHidden() {
    String name = hdfsPath.getName();
    if (name == null) return false;
    return name.startsWith("_") || name.startsWith(".");
  }

  @Override
  public long lastModified() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public void deleteOnExit() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public String[] list() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public String[] list(FilenameFilter filter) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public File[] listFiles(FilenameFilter filter) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean mkdir() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setLastModified(long time) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setReadOnly() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setWritable(boolean writable, boolean ownerOnly) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setWritable(boolean writable) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setReadable(boolean readable, boolean ownerOnly) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setReadable(boolean readable) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setExecutable(boolean executable, boolean ownerOnly) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean setExecutable(boolean executable) {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public boolean canExecute() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public long getTotalSpace() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public long getUsableSpace() {
    throw new UnsupportedOperationException(UNSUPPORT_OPERATION);
  }

  @Override
  public java.nio.file.Path toPath() {
    return Paths.get(toURI());
  }

  public BufferedReader getBufferedReader(String filePath) {
    try {
      return new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))));
    } catch (IOException e) {
      logger.error("Failed to get buffered reader for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    try {
      Path path = new Path(filePath);
      return new BufferedWriter(
          new OutputStreamWriter(append && fs.exists(path) ? fs.append(path) : fs.create(path)));
    } catch (IOException e) {
      logger.error("Failed to get buffered writer for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedInputStream getBufferedInputStream(String filePath) {
    try {
      return new BufferedInputStream(fs.open(new Path(filePath)));
    } catch (IOException e) {
      logger.error("Failed to get buffered input stream for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    try {
      return new BufferedOutputStream(fs.create(new Path(filePath)));
    } catch (IOException e) {
      logger.error("Failed to get buffered output stream for {}. ", filePath, e);
      return null;
    }
  }

  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    PathFilter pathFilter = path -> path.toUri().toString().endsWith(suffix);
    List<HDFSFile> files = listFiles(fileFolder, pathFilter);
    return files.toArray(new HDFSFile[0]);
  }

  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    PathFilter pathFilter = path -> path.toUri().toString().startsWith(prefix);
    List<HDFSFile> files = listFiles(fileFolder, pathFilter);
    return files.toArray(new HDFSFile[0]);
  }

  private List<HDFSFile> listFiles(String fileFolder, PathFilter pathFilter) {
    List<HDFSFile> files = new ArrayList<>();
    try {
      Path path = new Path(fileFolder);
      for (FileStatus fileStatus : fs.listStatus(path)) {
        Path filePath = fileStatus.getPath();
        if (pathFilter.accept(filePath)) {
          HDFSFile file = new HDFSFile(filePath.toUri().toString());
          files.add(file);
        }
      }
    } catch (IOException e) {
      logger.error("Failed to list files in {}. ", fileFolder);
    }
    return files;
  }

  public void moveTo(File dst) {
    try {
      fs.rename(this.hdfsPath, new Path(dst.getAbsolutePath()));
    } catch (IOException e) {
      logger.error("Failed to move file {} to {}. ", this, dst);
    }
  }

  public void moveFromLocalFile(File localSrc) {
    try {
      fs.moveFromLocalFile(new Path(localSrc.getAbsolutePath()), this.hdfsPath);
    } catch (IOException e) {
      logger.error("Failed to move local file {} to hdfs {}. ", localSrc, this);
    }
  }

  public void moveToLocalFile(File localDst) {
    try {
      fs.moveToLocalFile(this.hdfsPath, new Path(localDst.getAbsolutePath()));
    } catch (IOException e) {
      logger.error("Failed to move hdfs file {} to local {}. ", this, localDst);
    }
  }

  public void copyTo(File dst) {
    try {
      FileUtil.copy(fs, this.hdfsPath, fs, ((HDFSFile) dst).hdfsPath, false, fs.getConf());
    } catch (IOException e) {
      logger.error("Failed to copy {} to {}. ", this, dst);
    }
  }

  public void copyFromLocalFile(File localSrc) {
    try {
      fs.copyFromLocalFile(false, new Path(localSrc.getAbsolutePath()), this.hdfsPath);
    } catch (IOException e) {
      logger.error("Failed to copy local file {} to hdfs {}. ", localSrc, this);
    }
  }

  public void copyToLocalFile(File localDst) {
    try {
      fs.copyToLocalFile(false, this.hdfsPath, new Path(localDst.getAbsolutePath()));
    } catch (IOException e) {
      logger.error("Failed to copy hdfs file {} to local {}. ", this, localDst);
    }
  }
}
