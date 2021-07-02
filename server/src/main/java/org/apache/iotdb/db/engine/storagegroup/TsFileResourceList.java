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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.exception.WriteLockFailedException;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TsFileResourceList implements List<TsFileResource> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileResourceList.class);
  private TsFileResource header;
  private TsFileResource tail;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private int count = 0;

  public void readLock() {
    lock.readLock().lock();
  }

  public void readUnlock() {
    lock.readLock().unlock();
  }

  public void writeLock() {
    lock.writeLock().lock();
  }

  public boolean tryWriteLock() {
    return lock.writeLock().tryLock();
  }

  /**
   * Acquire write lock with timeout, {@link WriteLockFailedException} will be thrown after timeout.
   * The unit of timeout is ms.
   */
  public void writeLockWithTimeout(long timeout) throws WriteLockFailedException {
    try {
      if (lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
      } else {
        throw new WriteLockFailedException(
            String.format("cannot get write lock in %d ms", timeout));
      }
    } catch (InterruptedException e) {
      LOGGER.warn(e.getMessage(), e);
      Thread.interrupted();
      throw new WriteLockFailedException("thread is interrupted");
    }
  }

  public void writeUnlock() {
    lock.writeLock().unlock();
  }

  /**
   * Insert a new node before an existing node
   *
   * @param node the existing node
   * @param newNode the file to insert
   */
  public void insertBefore(TsFileResource node, TsFileResource newNode) {
    writeLock();
    try {
      newNode.prev = node.prev;
      newNode.next = node;
      if (node.prev == null) {
        header = newNode;
      } else {
        node.prev.next = newNode;
      }
      node.prev = newNode;
      count++;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Insert a new node after an existing node
   *
   * @param node the existing node
   * @param newNode the file to insert
   */
  public void insertAfter(TsFileResource node, TsFileResource newNode) {
    writeLock();
    try {
      newNode.prev = node;
      newNode.next = node.next;
      if (node.next == null) {
        tail = newNode;
      } else {
        node.next.prev = newNode;
      }
      node.next = newNode;
      count++;
    } finally {
      writeUnlock();
    }
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public boolean isEmpty() {
    return count == 0;
  }

  @Override
  public boolean contains(Object o) {
    readLock();
    try {
      if (!(o instanceof TsFileResource)) {
        return false;
      }
      boolean contain = false;
      TsFileResource current = header;
      while (current != null) {
        if (current.equals(o)) {
          contain = true;
          break;
        }
        current = current.next;
      }
      return contain;
    } finally {
      readUnlock();
    }
  }

  @Override
  public Iterator<TsFileResource> iterator() {
    return new TsFileIterator();
  }

  public Iterator<TsFileResource> reverseIterator() {
    return new TsFileReverseIterator();
  }

  /** Insert a new tsFileResource node to the end of List */
  @Override
  public boolean add(TsFileResource newNode) {
    writeLock();
    try {
      if (newNode.prev != null || newNode.next != null) {
        // this node already in a list
        return false;
      }
      if (tail == null) {
        header = newNode;
        tail = newNode;
        count++;
      } else {
        insertAfter(tail, newNode);
      }
      return true;
    } finally {
      writeUnlock();
    }
  }

  /**
   * The tsFileResourceListNode to be removed must be in the list, otherwise may cause unknown
   * behavior
   */
  @Override
  public boolean remove(Object o) {
    writeLock();
    try {
      TsFileResource tsFileResource = (TsFileResource) o;
      if (!contains(o)) {
        // the tsFileResource does not exist in this list
        return false;
      }
      if (tsFileResource.prev == null) {
        // remove header
        header = header.next;
        if (header != null) {
          header.prev = null;
        } else {
          // if list contains only one item, remove the header and the tail
          tail = null;
        }
      } else if (tsFileResource.next == null) {
        // remove tail
        tail = tail.prev;
        if (tail != null) {
          tail.next = null;
        } else {
          // if list contains only one item, remove the header and the tail
          header = null;
        }
      } else {
        tsFileResource.prev.next = tsFileResource.next;
        tsFileResource.next.prev = tsFileResource.prev;
      }
      tsFileResource.prev = null;
      tsFileResource.next = null;
      count--;
      return true;
    } finally {
      writeUnlock();
    }
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  /** Only List type parameter is legal, because it is in order. */
  @Override
  public boolean addAll(Collection<? extends TsFileResource> c) {
    writeLock();
    try {
      if (c instanceof List) {
        for (TsFileResource resource : c) {
          add(resource);
        }
        return true;
      }
      throw new NotImplementedException();
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void clear() {
    writeLock();
    try {
      header = null;
      tail = null;
      count = 0;
    } finally {
      writeUnlock();
    }
  }

  @Override
  public Object[] toArray() {
    throw new NotImplementedException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new NotImplementedException();
  }

  @Override
  public boolean addAll(int index, Collection<? extends TsFileResource> c) {
    throw new NotImplementedException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new NotImplementedException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new NotImplementedException();
  }

  @Override
  public TsFileResource get(int index) {
    int currIndex = 0;
    TsFileResource currTsFileResource = header;
    while (currIndex != index) {
      if (currTsFileResource.next == null) {
        throw new ArrayIndexOutOfBoundsException(currIndex);
      } else {
        currTsFileResource = currTsFileResource.next;
      }
      currIndex++;
    }
    return currTsFileResource;
  }

  /**
   * insert tsFileResource to a target pos(targetPos = index) e.g. if index = 0, then to the first,
   * if index = 1, then to the second.
   */
  @Override
  public TsFileResource set(int index, TsFileResource element) {
    int currIndex = 0;
    TsFileResource currTsFileResource = header;
    while (currIndex != index) {
      if (currTsFileResource.next == null) {
        if (currIndex == index - 1) {
          insertAfter(currTsFileResource, element);
          return element;
        } else {
          throw new ArrayIndexOutOfBoundsException(currIndex);
        }
      } else {
        currTsFileResource = currTsFileResource.next;
      }
      currIndex++;
    }
    if (currTsFileResource != null) {
      insertBefore(currTsFileResource, element);
    } else {
      add(element);
    }
    return element;
  }

  @Override
  public void add(int index, TsFileResource element) {
    throw new NotImplementedException();
  }

  @Override
  public TsFileResource remove(int index) {
    throw new NotImplementedException();
  }

  @Override
  public int indexOf(Object o) {
    throw new NotImplementedException();
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new NotImplementedException();
  }

  @Override
  public ListIterator<TsFileResource> listIterator() {
    throw new NotImplementedException();
  }

  @Override
  public ListIterator<TsFileResource> listIterator(int index) {
    throw new NotImplementedException();
  }

  @Override
  public List<TsFileResource> subList(int fromIndex, int toIndex) {
    throw new NotImplementedException();
  }

  public List<TsFileResource> getArrayList() {
    readLock();
    try {
      List<TsFileResource> list = new ArrayList<>();
      if (header == null) {
        return list;
      }
      TsFileResource current = header;
      while (current.next != null) {
        list.add(current);
        current = current.next;
      }
      list.add(current);
      return list;
    } finally {
      readUnlock();
    }
  }

  private class TsFileIterator implements Iterator<TsFileResource> {
    TsFileResource current;

    public TsFileIterator() {
      this.current = header;
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public TsFileResource next() {
      TsFileResource temp = current;
      current = current.next;
      return temp;
    }
  }

  private class TsFileReverseIterator implements Iterator<TsFileResource> {
    TsFileResource current;

    public TsFileReverseIterator() {
      current = tail;
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public TsFileResource next() {
      TsFileResource temp = current;
      current = current.prev;
      return temp;
    }
  }

  @TestOnly
  public TsFileResource getHeader() {
    return header;
  }

  @TestOnly
  public TsFileResource getTail() {
    return tail;
  }
}
