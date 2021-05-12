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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * This class references the implement of {@code sun.nio.fs.UnixPath}. The string format of this
 * class is like uri.toString() and the schema is hdfs.
 */
class HDFSPath implements Path {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  public static final char separator = '/';

  private static final String UNSUPPORTED_OPERATION = "Unsupported operation.";

  private final HDFSFileSystem hdfsFileSystem;
  /** path part of uri object */
  private final byte[] path;
  /** array of offsets of elements in path (created lazily) */
  private volatile int[] offsets;
  /** cached uri (created lazily) */
  private volatile URI uri;
  /** String representation (created lazily) */
  private volatile String stringValue;
  /** cached hashcode (created lazily) */
  private volatile int hash;

  HDFSPath(HDFSFileSystem hdfsFileSystem, String path) {
    this.hdfsFileSystem = hdfsFileSystem;
    this.path = normalizeAndCheck(path);
  }

  HDFSPath(HDFSFileSystem hdfsFileSystem, URI uri) {
    this.hdfsFileSystem = hdfsFileSystem;
    this.path = normalizeAndCheck(uri.getPath());
    this.uri = copyUriAndResetPath(uri, toString());
  }

  /** Converts {@code Path} to {@code HDFSPath} */
  static HDFSPath toHDFSPath(Path path) {
    if (path == null) throw new NullPointerException();
    if (!(path instanceof HDFSPath)) throw new ProviderMismatchException();
    return (HDFSPath) path;
  }

  /** Uses path param and uri's other elements to format uri */
  static URI copyUriAndResetPath(URI uri, String path) {
    try {
      return new URI(
          uri.getScheme(),
          uri.getUserInfo(),
          uri.getHost(),
          uri.getPort(),
          path,
          uri.getQuery(),
          uri.getFragment());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Normalizes path, including removing redundant separators and making sure no character is NUL
   * (\u0000).
   */
  private static byte[] normalizeAndCheck(String input) {
    int len = input.length();
    char prevChar = 0;
    for (int i = 0; i < len; i++) {
      char c = input.charAt(i);
      // remove redundant separators behind i - 1
      if ((c == separator) && (prevChar == separator)) return normalize(input, len, i - 1);
      checkNotNul(c, input);
      prevChar = c;
    }
    // only root path ends with separator, so remove last separator
    if (prevChar == separator) return normalize(input, len, len - 1);
    return input.getBytes(DEFAULT_CHARSET);
  }

  private static byte[] normalize(String input, int len, int off) {
    if (len == 0) return input.getBytes(DEFAULT_CHARSET);
    int n = len;
    // remove last separators
    while ((n > 0) && (input.charAt(n - 1) == separator)) n--;
    if (n == 0) return new byte[] {separator};
    // remove redundant separators in [off, n)
    StringBuilder sb = new StringBuilder(input.length());
    if (off > 0) sb.append(input, 0, off);
    char prevChar = 0;
    for (int i = off; i < n; i++) {
      char c = input.charAt(i);
      if ((c == separator) && (prevChar == separator)) continue;
      checkNotNul(c, input);
      sb.append(c);
      prevChar = c;
    }
    return sb.toString().getBytes(DEFAULT_CHARSET);
  }

  private static void checkNotNul(char c, String pathMayInvalid) {
    if (c == '\u0000') throw new InvalidPathException(pathMayInvalid, "Nul character not allowed");
  }

  /** Finds offsets of separators */
  private void initOffsets() {
    if (offsets != null) {
      return;
    }

    // step1: count names
    int count = 0;
    int index = 0;
    if (isEmpty()) {
      // empty path has one name
      count = 1;
    } else {
      while (index < path.length) {
        byte c = path[index++];
        if (c != separator) {
          count++;
          while (index < path.length && path[index] != separator) index++;
        }
      }
    }

    // step2: populate offsets
    int[] result = new int[count];
    count = 0;
    index = 0;
    while (index < path.length) {
      byte c = path[index];
      if (c == separator) {
        index++;
      } else {
        result[count++] = index++;
        while (index < path.length && path[index] != separator) index++;
      }
    }
    synchronized (this) {
      if (offsets == null) offsets = result;
    }
  }

  /** Returns true if this path is an empty path */
  private boolean isEmpty() {
    return path.length == 0;
  }

  /** Returns an empty path */
  private HDFSPath emptyPath() {
    return new HDFSPath(getFileSystem(), "");
  }

  @Override
  public HDFSFileSystem getFileSystem() {
    return hdfsFileSystem;
  }

  @Override
  public boolean isAbsolute() {
    return (path.length > 0 && path[0] == separator);
  }

  @Override
  public HDFSPath getRoot() {
    if (isAbsolute()) {
      return getFileSystem().rootDirectory();
    } else {
      return null;
    }
  }

  @Override
  public HDFSPath getFileName() {
    initOffsets();

    int count = offsets.length;

    // no elements so no name
    if (count == 0) return null;

    // one name element and no root component
    if (count == 1 && path.length > 0 && path[0] != separator) return this;

    int lastOffset = offsets[count - 1];
    int len = path.length - lastOffset;
    byte[] result = new byte[len];
    System.arraycopy(path, lastOffset, result, 0, len);
    return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
  }

  @Override
  public HDFSPath getParent() {
    initOffsets();

    int count = offsets.length;
    if (count == 0) {
      // no elements so no parent
      return null;
    }
    int len = offsets[count - 1] - 1;
    if (len <= 0) {
      // parent is root only (may be null)
      return getRoot();
    }
    byte[] result = new byte[len];
    System.arraycopy(path, 0, result, 0, len);
    return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
  }

  @Override
  public int getNameCount() {
    initOffsets();
    return offsets.length;
  }

  @Override
  public HDFSPath getName(int index) {
    initOffsets();
    if (index < 0 || index >= offsets.length) throw new IllegalArgumentException();

    int begin = offsets[index];
    int len;
    if (index == (offsets.length - 1)) {
      len = path.length - begin;
    } else {
      len = offsets[index + 1] - begin - 1;
    }
    byte[] result = new byte[len];
    System.arraycopy(path, begin, result, 0, len);
    return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
  }

  @Override
  public HDFSPath subpath(int beginIndex, int endIndex) {
    initOffsets();
    initOffsets();
    if (beginIndex < 0
        || beginIndex >= offsets.length
        || endIndex > offsets.length
        || beginIndex >= endIndex) throw new IllegalArgumentException();

    int begin = offsets[beginIndex];
    int len;
    if (endIndex == offsets.length) {
      len = path.length - begin;
    } else {
      len = offsets[endIndex] - begin - 1;
    }
    byte[] result = new byte[len];
    System.arraycopy(path, begin, result, 0, len);
    return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
  }

  @Override
  public boolean startsWith(Path other) {
    if (!(Objects.requireNonNull(other) instanceof HDFSPath)) return false;
    HDFSPath that = (HDFSPath) other;

    // other path is longer
    if (that.path.length > path.length) return false;

    int thisOffsetCount = getNameCount();
    int thatOffsetCount = that.getNameCount();

    // other path has no name elements
    if (thatOffsetCount == 0 && this.isAbsolute()) {
      return !that.isEmpty();
    }

    // given path has more elements that this path
    if (thatOffsetCount > thisOffsetCount) return false;

    // same number of elements so must be exact match
    if ((thatOffsetCount == thisOffsetCount) && (path.length != that.path.length)) {
      return false;
    }

    // check offsets of elements match
    for (int i = 0; i < thatOffsetCount; i++) {
      Integer o1 = offsets[i];
      Integer o2 = that.offsets[i];
      if (!o1.equals(o2)) return false;
    }

    // offsets match so need to compare bytes
    int i = 0;
    while (i < that.path.length) {
      if (this.path[i] != that.path[i]) return false;
      i++;
    }

    // final check that match is on name boundary
    if (i < path.length && this.path[i] != separator) return false;

    return true;
  }

  @Override
  public boolean startsWith(String other) {
    return startsWith(getFileSystem().getPath(other));
  }

  @Override
  public boolean endsWith(Path other) {
    if (!(Objects.requireNonNull(other) instanceof HDFSPath)) return false;
    HDFSPath that = (HDFSPath) other;

    int thisLen = path.length;
    int thatLen = that.path.length;

    // other path is longer
    if (thatLen > thisLen) return false;

    // other path is the empty path
    if (thisLen > 0 && thatLen == 0) return false;

    // other path is absolute so this path must be absolute
    if (that.isAbsolute() && !this.isAbsolute()) return false;

    int thisOffsetCount = getNameCount();
    int thatOffsetCount = that.getNameCount();

    // given path has more elements that this path
    if (thatOffsetCount > thisOffsetCount) {
      return false;
    } else {
      // same number of elements
      if (thatOffsetCount == thisOffsetCount) {
        if (thisOffsetCount == 0) return true;
        int expectedLen = thisLen;
        if (this.isAbsolute() && !that.isAbsolute()) expectedLen--;
        if (thatLen != expectedLen) return false;
      } else {
        // this path has more elements so given path must be relative
        if (that.isAbsolute()) return false;
      }
    }

    // compare bytes
    int thisPos = offsets[thisOffsetCount - thatOffsetCount];
    int thatPos = that.offsets[0];
    if ((thatLen - thatPos) != (thisLen - thisPos)) return false;
    while (thatPos < thatLen) {
      if (this.path[thisPos++] != that.path[thatPos++]) return false;
    }

    return true;
  }

  @Override
  public boolean endsWith(String other) {
    return endsWith(getFileSystem().getPath(other));
  }

  @Override
  public HDFSPath normalize() {
    final int count = getNameCount();
    if (count == 0 || isEmpty()) return this;

    boolean[] ignore = new boolean[count]; // true => ignore name
    int[] size = new int[count]; // length of name
    int remaining = count; // number of names remaining
    boolean hasDotDot = false; // has at least one ..
    boolean isAbsolute = isAbsolute();

    // first pass:
    //   1. compute length of names
    //   2. mark all occurrences of "." to ignore
    //   3. and look for any occurrences of ".."
    for (int i = 0; i < count; i++) {
      int begin = offsets[i];
      int len;
      if (i == (offsets.length - 1)) {
        len = path.length - begin;
      } else {
        len = offsets[i + 1] - begin - 1;
      }
      size[i] = len;

      if (path[begin] == '.') {
        if (len == 1) {
          ignore[i] = true; // ignore  "."
          remaining--;
        } else {
          if (path[begin + 1] == '.') // ".." found
          hasDotDot = true;
        }
      }
    }

    // multiple passes to eliminate all occurrences of name/..
    if (hasDotDot) {
      int prevRemaining;
      do {
        prevRemaining = remaining;
        int prevName = -1;
        for (int i = 0; i < count; i++) {
          if (ignore[i]) continue;

          // not a ".."
          if (size[i] != 2) {
            prevName = i;
            continue;
          }

          int begin = offsets[i];
          if (path[begin] != '.' || path[begin + 1] != '.') {
            prevName = i;
            continue;
          }

          // ".." found
          if (prevName >= 0) {
            // name/<ignored>/.. found so mark name and ".." to be
            // ignored
            ignore[prevName] = true;
            ignore[i] = true;
            remaining = remaining - 2;
            prevName = -1;
          } else {
            // Case: /<ignored>/.. so mark ".." as ignored
            if (isAbsolute) {
              boolean hasPrevious = false;
              for (int j = 0; j < i; j++) {
                if (!ignore[j]) {
                  hasPrevious = true;
                  break;
                }
              }
              if (!hasPrevious) {
                // all proceeding names are ignored
                ignore[i] = true;
                remaining--;
              }
            }
          }
        }
      } while (prevRemaining > remaining);
    }

    // no redundant names
    if (remaining == count) return this;

    // corner case - all names removed
    if (remaining == 0) {
      return isAbsolute ? getFileSystem().rootDirectory() : emptyPath();
    }

    // compute length of result
    int len = remaining - 1;
    if (isAbsolute) len++;

    for (int i = 0; i < count; i++) {
      if (!ignore[i]) len += size[i];
    }
    byte[] result = new byte[len];

    // copy names into result
    int pos = 0;
    if (isAbsolute) result[pos++] = separator;
    for (int i = 0; i < count; i++) {
      if (!ignore[i]) {
        System.arraycopy(path, offsets[i], result, pos, size[i]);
        pos += size[i];
        if (--remaining > 0) {
          result[pos++] = separator;
        }
      }
    }
    return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
  }

  @Override
  public HDFSPath resolve(Path other) {
    byte[] otherPath = toHDFSPath(other).path;
    if (otherPath.length > 0 && otherPath[0] == '/') return ((HDFSPath) other);
    byte[] result = resolve(path, otherPath);
    return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
  }

  /** Resolves child against given base */
  private byte[] resolve(byte[] base, byte[] child) {
    int baseLength = base.length;
    int childLength = child.length;
    if (childLength == 0) return base;
    if (baseLength == 0 || child[0] == separator) return child;
    byte[] result;
    if (baseLength == 1 && base[0] == separator) {
      result = new byte[childLength + 1];
      result[0] = separator;
      System.arraycopy(child, 0, result, 1, childLength);
    } else {
      result = new byte[baseLength + 1 + childLength];
      System.arraycopy(base, 0, result, 0, baseLength);
      result[base.length] = separator;
      System.arraycopy(child, 0, result, baseLength + 1, childLength);
    }
    return result;
  }

  @Override
  public HDFSPath resolve(String other) {
    return resolve(getFileSystem().getPath(other));
  }

  @Override
  public Path resolveSibling(Path other) {
    if (other == null) throw new NullPointerException();
    Path parent = getParent();
    return (parent == null) ? other : parent.resolve(other);
  }

  @Override
  public Path resolveSibling(String other) {
    return resolveSibling(getFileSystem().getPath(other));
  }

  @Override
  public HDFSPath relativize(Path other) {
    HDFSPath that = toHDFSPath(other);
    if (that.equals(this)) return emptyPath();

    // can only relativize paths of the same type
    if (this.isAbsolute() != that.isAbsolute())
      throw new IllegalArgumentException("'other' is different type of Path");

    // this path is the empty path
    if (this.isEmpty()) return that;

    int bn = this.getNameCount();
    int cn = that.getNameCount();

    // skip matching names
    int n = (bn > cn) ? cn : bn;
    int i = 0;
    while (i < n) {
      if (!this.getName(i).equals(that.getName(i))) break;
      i++;
    }

    int dotdots = bn - i;
    if (i < cn) {
      // remaining name components in other
      HDFSPath remainder = that.subpath(i, cn);
      if (dotdots == 0) return remainder;

      // other is the empty path
      boolean isOtherEmpty = that.isEmpty();

      // result is a  "../" for each remaining name in base
      // followed by the remaining names in other. If the remainder is
      // the empty path then we don't add the final trailing slash.
      int len = dotdots * 3 + remainder.path.length;
      if (isOtherEmpty) {
        assert remainder.isEmpty();
        len--;
      }
      byte[] result = new byte[len];
      int pos = 0;
      while (dotdots > 0) {
        result[pos++] = (byte) '.';
        result[pos++] = (byte) '.';
        if (isOtherEmpty) {
          if (dotdots > 1) result[pos++] = (byte) separator;
        } else {
          result[pos++] = (byte) separator;
        }
        dotdots--;
      }
      System.arraycopy(remainder.path, 0, result, pos, remainder.path.length);
      return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
    } else {
      // no remaining names in other so result is simply a sequence of ".."
      byte[] result = new byte[dotdots * 3 - 1];
      int pos = 0;
      while (dotdots > 0) {
        result[pos++] = (byte) '.';
        result[pos++] = (byte) '.';
        // no tailing slash at the end
        if (dotdots > 1) result[pos++] = (byte) separator;
        dotdots--;
      }
      return new HDFSPath(getFileSystem(), new String(result, DEFAULT_CHARSET));
    }
  }

  @Override
  public URI toUri() {
    // OK if two or more threads create a String
    if (uri == null) {
      if (isAbsolute()) {
        // use root directory's uri elements
        uri = copyUriAndResetPath(hdfsFileSystem.rootDirectory().toUri(), toString());
      } else {
        uri = toAbsolutePath().toUri();
      }
    }
    return uri;
  }

  @Override
  public Path toAbsolutePath() {
    if (isAbsolute()) {
      return this;
    }
    // The path is relative so need to resolve against default directory
    return new HDFSPath(getFileSystem(), getFileSystem().userDirectory().resolve(this).toString());
  }

  @Override
  public Path toRealPath(LinkOption... options) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  @Override
  public File toFile() {
    return new HDFSFile(toUri());
  }

  @Override
  public WatchKey register(
      WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers)
      throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
  }

  @Override
  public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
    return register(watcher, events, new WatchEvent.Modifier[0]);
  }

  @Override
  public final Iterator<Path> iterator() {
    return new Iterator<Path>() {
      private int idx = 0;

      @Override
      public boolean hasNext() {
        return (idx < getNameCount());
      }

      @Override
      public Path next() {
        if (idx < getNameCount()) {
          Path result = getName(idx);
          idx++;
          return result;
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public int compareTo(Path other) {
    int len1 = this.path.length;
    int len2 = ((HDFSPath) other).path.length;

    int n = Math.min(len1, len2);
    byte[] v1 = this.path;
    byte[] v2 = ((HDFSPath) other).path;

    int k = 0;
    while (k < n) {
      int c1 = v1[k] & 0xff;
      int c2 = v2[k] & 0xff;
      if (c1 != c2) {
        return c1 - c2;
      }
      k++;
    }
    return len1 - len2;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof HDFSPath) {
      return compareTo((Path) other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    // OK if two or more threads compute hash
    int h = hash;
    if (h == 0) {
      for (byte b : path) {
        h = 31 * h + (b & 0xff);
      }
      hash = h;
    }
    return h;
  }

  @Override
  public String toString() {
    // OK if two or more threads create a String
    if (stringValue == null) {
      stringValue = new String(path, DEFAULT_CHARSET);
    }
    return stringValue;
  }

  public org.apache.hadoop.fs.Path toHadoopPath() {
    return new org.apache.hadoop.fs.Path(toUri());
  }
}
