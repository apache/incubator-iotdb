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

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HDFSFileSystemProvider extends FileSystemProvider {
  private final Map<String, HDFSFileSystem> fileSystems = new WeakHashMap<>();
  private final ReadWriteLock fileSystemsLock = new ReentrantReadWriteLock();

  private void checkUri(URI uri) {
    if (!uri.getScheme().equalsIgnoreCase(getScheme()))
      throw new IllegalArgumentException("URI does not match this provider");
    if (uri.getPath() == null) throw new IllegalArgumentException("Path component is undefined");
  }

  private String getKey(URI uri) {
    return uri.getHost() + ":" + uri.getPort();
  }

  private HDFSFileSystem getFileSystem(Path path) {
    return HDFSPath.toHDFSPath(path).getFileSystem();
  }

  @Override
  public String getScheme() {
    return "hdfs";
  }

  @Override
  public HDFSFileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
    checkUri(uri);
    fileSystemsLock.writeLock().lock();
    try {
      String key = getKey(uri);
      HDFSFileSystem fs = fileSystems.get(key);
      if (fs != null && fs.isOpen()) {
        throw new FileSystemAlreadyExistsException();
      }
      fs = new HDFSFileSystem(this, uri);
      fileSystems.put(key, fs);
      return fs;
    } finally {
      fileSystemsLock.writeLock().unlock();
    }
  }

  @Override
  public HDFSFileSystem getFileSystem(URI uri) {
    checkUri(uri);
    String key = getKey(uri);
    fileSystemsLock.readLock().lock();
    try {
      HDFSFileSystem fs = fileSystems.get(key);
      if (fs == null || !fs.isOpen()) {
        throw new FileSystemNotFoundException();
      }
      return fs;
    } finally {
      fileSystemsLock.readLock().unlock();
    }
  }

  @Override
  public Path getPath(URI uri) {
    checkUri(uri);
    HDFSFileSystem fs;
    try {
      fs = getFileSystem(uri);
    } catch (FileSystemNotFoundException e) {
      // create when not found
      fileSystemsLock.writeLock().lock();
      try {
        String key = getKey(uri);
        fs = fileSystems.get(key);
        if (fs == null || !fs.isOpen()) {
          try {
            fs = new HDFSFileSystem(this, uri);
          } catch (IOException ioException) {
            throw e;
          }
          fileSystems.put(key, fs);
        }
      } finally {
        fileSystemsLock.writeLock().unlock();
      }
    }
    return fs.getPath(uri.getPath());
  }

  @Override
  public SeekableByteChannel newByteChannel(
      Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    return getFileSystem(path).newByteChannel(path, options, attrs);
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(
      Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
    return getFileSystem(dir).newDirectoryStream(dir, filter);
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    getFileSystem(dir).createDirectory(dir, attrs);
  }

  @Override
  public void delete(Path path) throws IOException {
    getFileSystem(path).delete(path, false);
  }

  @Override
  public void copy(Path source, Path target, CopyOption... options) throws IOException {
    getFileSystem(source).copy(source, target, options);
  }

  @Override
  public void move(Path source, Path target, CopyOption... options) throws IOException {
    getFileSystem(source).move(source, target, options);
  }

  @Override
  public boolean isSameFile(Path path, Path path2) throws IOException {
    return getFileSystem(path).isSameFile(path, path2);
  }

  @Override
  public boolean isHidden(Path path) throws IOException {
    return getFileSystem(path).isHidden(path);
  }

  @Override
  public FileStore getFileStore(Path path) throws IOException {
    return getFileSystem(path).getFileStore(path);
  }

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    getFileSystem(path).checkAccess(path, modes);
  }

  @Override
  public <V extends FileAttributeView> V getFileAttributeView(
      Path path, Class<V> type, LinkOption... options) {
    return getFileSystem(path).getFileAttributeView(path, type, options);
  }

  @Override
  public <A extends BasicFileAttributes> A readAttributes(
      Path path, Class<A> type, LinkOption... options) throws IOException {
    return getFileSystem(path).readAttributes(path, type, options);
  }

  @Override
  public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
      throws IOException {
    return getFileSystem(path).readAttributes(path, attributes, options);
  }

  @Override
  public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
      throws IOException {
    getFileSystem(path).setAttribute(path, attribute, value, options);
  }
}
