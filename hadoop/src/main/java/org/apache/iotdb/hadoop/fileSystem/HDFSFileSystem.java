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

import org.apache.iotdb.hadoop.utils.Globs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.iotdb.hadoop.fileSystem.HDFSPath.separator;

class HDFSFileSystem extends FileSystem {

  private static final Logger logger = LoggerFactory.getLogger(HDFSFileSystem.class);
  private static final String UNSUPPORTED_OPERATION = "Unsupported operation.";

  private final HDFSFileSystemProvider provider;
  private final HDFSPath rootDirectory;
  private final HDFSPath userDirectory;
  /** connector to hdfs */
  private org.apache.hadoop.fs.FileSystem hdfs;

  private volatile boolean isOpen = true;

  protected HDFSFileSystem(HDFSFileSystemProvider provider, URI dir) throws IOException {
    this.provider = provider;
    Configuration conf = HDFSConfUtil.setConf(new Configuration());
    try {
      this.hdfs = org.apache.hadoop.fs.FileSystem.get(dir, conf);
    } catch (IOException e) {
      logger.error(
          String.format(
              "Fail to get org.apache.hadoop.fs.FileSystem for %s. Please check your hadoop.",
              dir.toString()));
      throw e;
    }
    this.rootDirectory = new HDFSPath(this, HDFSPath.copyUriAndResetPath(dir, getSeparator()));
    // use root dir as default user dir
    this.userDirectory = this.rootDirectory;
  }

  HDFSPath userDirectory() {
    return userDirectory;
  }

  HDFSPath rootDirectory() {
    return rootDirectory;
  }

  @Override
  public FileSystemProvider provider() {
    return provider;
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    this.hdfs.close();
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public String getSeparator() {
    return String.valueOf(separator);
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    return Collections.singletonList(rootDirectory);
  }

  @Override
  public Iterable<FileStore> getFileStores() {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  @Override
  public Set<String> supportedFileAttributeViews() {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  @Override
  public Path getPath(String first, String... more) {
    String path;
    if (more.length == 0) {
      path = first;
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(first);
      for (String segment : more) {
        if (segment.length() > 0) {
          if (sb.length() > 0) sb.append(separator);
          sb.append(segment);
        }
      }
      path = sb.toString();
    }
    return new HDFSPath(this, path);
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    int pos = syntaxAndPattern.indexOf(':');
    if (pos <= 0 || pos == syntaxAndPattern.length()) {
      throw new IllegalArgumentException();
    }
    String syntax = syntaxAndPattern.substring(0, pos);
    String input = syntaxAndPattern.substring(pos + 1);
    String expr;
    if (syntax.equals(GLOB_SYNTAX)) {
      expr = Globs.toUnixRegexPattern(input);
    } else if (syntax.equals(REGEX_SYNTAX)) {
      expr = input;
    } else {
      throw new UnsupportedOperationException("Syntax '" + syntax + "' not recognized");
    }
    // return matcher
    final Pattern pattern = Pattern.compile(expr);
    return path -> pattern.matcher(path.toString()).matches();
  }

  private static final String GLOB_SYNTAX = "glob";
  private static final String REGEX_SYNTAX = "regex";

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    return LookupService.instance;
  }

  static class LookupService {
    static final UserPrincipalLookupService instance =
        new UserPrincipalLookupService() {
          @Override
          public UserPrincipal lookupPrincipalByName(String name) throws IOException {
            return HDFSUserPrinciples.lookupUser(name);
          }

          @Override
          public GroupPrincipal lookupPrincipalByGroupName(String group) throws IOException {
            return HDFSUserPrinciples.lookupGroup(group);
          }
        };
  }

  static class HDFSUserPrinciples {
    static class HDFSUser implements UserPrincipal {
      protected UserGroupInformation ugi;
      private final String name;

      public HDFSUser(String name) {
        this.ugi = UserGroupInformation.createRemoteUser(name);
        this.name = name;
      }

      @Override
      public String getName() {
        return this.ugi.getUserName();
      }

      @Override
      public int hashCode() {
        return name.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        } else if (obj == null || getClass() != obj.getClass()) {
          return false;
        } else if (!this.ugi.getUserName().equals(((HDFSUser) obj).ugi.getUserName())
            || !Arrays.equals(this.ugi.getGroupNames(), ((HDFSUser) obj).ugi.getGroupNames())) {
          return false;
        }
        return true;
      }
    }

    static class HDFSGroup extends HDFSUser implements GroupPrincipal {
      protected HDFSGroup(String name) {
        super(name);
      }
    }

    // lookup user name
    static UserPrincipal lookupUser(String name) throws IOException {
      return new HDFSUser(name);
    }

    // lookup group name
    static GroupPrincipal lookupGroup(String group) throws IOException {
      return new HDFSGroup(group);
    }
  }

  @Override
  public WatchService newWatchService() throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  // -- methods below are prepared for HDFSFileSystemProvider class --

  private void checkOpen() {
    if (!isOpen) {
      throw new ClosedFileSystemException();
    }
  }

  SeekableByteChannel newByteChannel(
      Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
      throws IOException {
    checkOpen();
    org.apache.hadoop.fs.Path hadoopDir = HDFSPath.toHDFSPath(dir).toHadoopPath();
    return new HDFSDirectoryStream(this, hadoopDir, filter);
  }

  static class HDFSDirectoryStream implements DirectoryStream<Path> {
    private final HDFSFileSystem hdfsFileSystem;
    private final org.apache.hadoop.fs.Path hadoopPath;
    private final Filter<? super Path> filter;
    private volatile boolean isClosed;
    private volatile Iterator<Path> itr;

    HDFSDirectoryStream(
        HDFSFileSystem hdfsFileSystem,
        org.apache.hadoop.fs.Path hadoopPath,
        Filter<? super Path> filter) {
      this.hdfsFileSystem = hdfsFileSystem;
      this.hadoopPath = hadoopPath;
      this.filter = filter;
    }

    @Override
    public synchronized Iterator<Path> iterator() {
      if (isClosed) {
        throw new ClosedDirectoryStreamException();
      }
      if (itr != null) {
        throw new IllegalStateException("Iterator has already been returned");
      }
      try {
        hdfsFileSystem.checkOpen();
        List<Path> list = new ArrayList<>();
        for (FileStatus stat : hdfsFileSystem.hdfs.listStatus(hadoopPath)) {
          HDFSPath hdfsPath = new HDFSPath(hdfsFileSystem, stat.getPath().toUri());
          if (filter == null || filter.accept(hdfsPath)) list.add(hdfsPath);
        }
        itr = list.iterator();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      return new Iterator<Path>() {
        @Override
        public boolean hasNext() {
          if (isClosed) {
            throw new ClosedDirectoryStreamException();
          }
          return itr.hasNext();
        }

        @Override
        public synchronized Path next() {
          if (isClosed) {
            throw new ClosedDirectoryStreamException();
          }
          return itr.next();
        }

        @Override
        public void remove() {
          if (isClosed) {
            throw new ClosedDirectoryStreamException();
          }
          itr.remove();
        }
      };
    }

    @Override
    public synchronized void close() throws IOException {
      isClosed = true;
    }
  }

  void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    checkOpen();
    org.apache.hadoop.fs.Path hadoopDir = HDFSPath.toHDFSPath(dir).toHadoopPath();
    if (hdfs.exists(hadoopDir)) {
      throw new FileAlreadyExistsException(dir.toString());
    }
    // ignore the attrs param
    hdfs.mkdirs(hadoopDir);
  }

  void delete(Path path, boolean recursive) throws IOException {
    checkOpen();
    org.apache.hadoop.fs.Path hadoopPath = HDFSPath.toHDFSPath(path).toHadoopPath();
    // If no exist
    if (!this.hdfs.exists(hadoopPath)) {
      return;
    }
    if (!recursive
        && this.hdfs.getFileStatus(hadoopPath).isDirectory()
        && this.hdfs.listStatus(hadoopPath).length > 0) {
      throw new DirectoryNotEmptyException(hadoopPath.toString());
    }
    this.hdfs.delete(hadoopPath, recursive);
  }

  void copy(Path source, Path target, CopyOption... options) throws IOException {
    checkOpen();
    org.apache.hadoop.fs.Path hadoopSource = HDFSPath.toHDFSPath(source).toHadoopPath();
    org.apache.hadoop.fs.Path hadoopTarget = HDFSPath.toHDFSPath(target).toHadoopPath();
    if (!hdfs.exists(hadoopSource)) {
      throw new NoSuchFileException(source.toString());
    }

    boolean hasReplace = false;
    for (CopyOption opt : options) {
      if (opt == StandardCopyOption.REPLACE_EXISTING) hasReplace = true;
      else throw new UnsupportedOperationException(opt.toString());
    }

    if (hdfs.exists(hadoopTarget)) {
      if (!hasReplace) {
        throw new FileAlreadyExistsException(target.toString());
      }
      if (hdfs.isDirectory(hadoopTarget) && hdfs.listStatus(hadoopTarget).length > 0) {
        throw new DirectoryNotEmptyException(target.toString());
      }
      hdfs.delete(hadoopTarget, false);
    }

    FileUtil.copy(hdfs, hadoopSource, hdfs, hadoopTarget, false, hdfs.getConf());
  }

  void move(Path source, Path target, CopyOption... options) throws IOException {
    checkOpen();
    org.apache.hadoop.fs.Path hadoopSource = HDFSPath.toHDFSPath(source).toHadoopPath();
    org.apache.hadoop.fs.Path hadoopTarget = HDFSPath.toHDFSPath(target).toHadoopPath();
    if (!hdfs.exists(hadoopSource)) {
      throw new NoSuchFileException(source.toString());
    }

    boolean hasReplace = false;
    for (CopyOption opt : options) {
      if (opt == StandardCopyOption.REPLACE_EXISTING) hasReplace = true;
      else throw new UnsupportedOperationException(opt.toString());
    }

    if (hdfs.exists(hadoopTarget)) {
      if (!hasReplace) {
        throw new FileAlreadyExistsException(target.toString());
      }
      if (hdfs.isDirectory(hadoopTarget) && hdfs.listStatus(hadoopTarget).length > 0) {
        throw new DirectoryNotEmptyException(target.toString());
      }
      hdfs.delete(hadoopTarget, false);
    }

    hdfs.rename(hadoopSource, hadoopTarget);
  }

  boolean isSameFile(Path path, Path path2) throws IOException {
    checkOpen();
    return path.equals(path2);
  }

  boolean isHidden(Path path) throws IOException {
    return HDFSPath.toHDFSPath(path).toFile().isHidden();
  }

  FileStore getFileStore(Path path) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  void checkAccess(Path path, AccessMode... modes) throws IOException {
    org.apache.hadoop.fs.Path hadoopPath = HDFSPath.toHDFSPath(path).toHadoopPath();
    // check file exists
    if (!hdfs.exists(hadoopPath)) {
      throw new NoSuchFileException(path.toString());
    }
    if (modes.length == 0) {
      return;
    }
    if (!hdfs.getConf().getBoolean("dfs.namenode.acls.enabled", false)) {
      throw new UnsupportedOperationException("HDFS ACLs has been disabled");
    }

    char[] posixMode = {'-', '-', '-'};
    for (AccessMode mode : modes) {
      switch (mode) {
        case READ:
          posixMode[0] = 'r';
          break;
        case WRITE:
          posixMode[1] = 'w';
          break;
        case EXECUTE:
          posixMode[2] = 'x';
          break;
        default:
          break;
      }
    }
    try {
      hdfs.access(hadoopPath, FsAction.getFsAction(String.valueOf(posixMode)));
    } catch (FileNotFoundException e) {
      throw new NoSuchFileException(path.toString());
    } catch (AccessControlException e) {
      throw new AccessDeniedException(path.toString());
    }
  }

  <V extends FileAttributeView> V getFileAttributeView(
      Path path, Class<V> type, LinkOption... options) {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
      throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
      throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  void setAttribute(Path path, String attribute, Object value, LinkOption... options)
      throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }
}
