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

package org.apache.iotdb.db.utils.datastructure;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * RandomAccessArrayDeque is a modified version of ArrayDeque from java.util, providing the ability
 * to access an element by index.
 * Most codes are copied directly because the fields are package-private and cannot be accessed
 * by a simple extension.
 * @param <E>
 */
@SuppressWarnings("SuspiciousSystemArraycopy")
public class RandomAccessArrayDeque<E> extends AbstractCollection<E>
    implements Deque<E>, RandomAccess
{
  /**
   * The array in which the elements of the deque are stored.
   * The capacity of the deque is the length of this array, which is
   * always a power of two. The array is never allowed to become
   * full, except transiently within an addX method where it is
   * resized (see doubleCapacity) immediately upon becoming full,
   * thus avoiding head and tail wrapping around to equal each
   * other.  We also guarantee that all array cells not holding
   * deque elements are always null.
   */
  Object[] elements; // non-private to simplify nested class access

  /**
   * The index of the element at the head of the deque (which is the
   * element that would be removed by remove() or pop()); or an
   * arbitrary number equal to tail if the deque is empty.
   */
  int head;

  /**
   * The index at which the next element would be added to the tail
   * of the deque (via addLast(E), add(E), or push(E)).
   */
  int tail;

  /**
   * The minimum capacity that we'll use for a newly created deque.
   * Must be a power of 2.
   */
  private static final int MIN_INITIAL_CAPACITY = 8;

  // ******  Array allocation and resizing utilities ******

  private static int calculateSize(int numElements) {
    int initialCapacity = MIN_INITIAL_CAPACITY;
    // Find the best power of two to hold elements.
    // Tests "<=" because arrays aren't kept full.
    if (numElements >= initialCapacity) {
      initialCapacity = numElements;
      initialCapacity |= (initialCapacity >>>  1);
      initialCapacity |= (initialCapacity >>>  2);
      initialCapacity |= (initialCapacity >>>  4);
      initialCapacity |= (initialCapacity >>>  8);
      initialCapacity |= (initialCapacity >>> 16);
      initialCapacity++;

      if (initialCapacity < 0)   // Too many elements, must back off
        initialCapacity >>>= 1;// Good luck allocating 2 ^ 30 elements
    }
    return initialCapacity;
  }

  /**
   * Allocates empty array to hold the given number of elements.
   *
   * @param numElements  the number of elements to hold
   */
  private void allocateElements(int numElements) {
    elements = new Object[calculateSize(numElements)];
  }

  /**
   * Doubles the capacity of this deque.  Call only when full, i.e.,
   * when head and tail have wrapped around to become equal.
   */
  private void doubleCapacity() {
    assert head == tail;
    int p = head;
    int n = elements.length;
    int r = n - p; // number of elements to the right of p
    int newCapacity = n << 1;
    if (newCapacity < 0)
      throw new IllegalStateException("Sorry, deque too big");
    Object[] a = new Object[newCapacity];
    System.arraycopy(elements, p, a, 0, r);
    System.arraycopy(elements, 0, a, r, p);
    elements = a;
    head = 0;
    tail = n;
  }

  /**
   * Copies the elements from our element array into the specified array,
   * in order (from first to last element in the deque).  It is assumed
   * that the array is large enough to hold all elements in the deque.
   *
   * @return its argument
   */
  private <T> T[] copyElements(T[] a) {
    if (head < tail) {
      System.arraycopy(elements, head, a, 0, size());
    } else if (head > tail) {
      int headPortionLen = elements.length - head;
      System.arraycopy(elements, head, a, 0, headPortionLen);
      System.arraycopy(elements, 0, a, headPortionLen, tail);
    }
    return a;
  }

  /**
   * Constructs an empty array deque with an initial capacity
   * sufficient to hold the specified number of elements.
   *
   * @param numElements  lower bound on initial capacity of the deque
   */
  public RandomAccessArrayDeque(int numElements) {
    allocateElements(numElements);
  }

  public E get(int index) {
    if (index >= size() || index < 0) {
      throw new ArrayIndexOutOfBoundsException(index);
    }

    int pos = (head + index) & (elements.length - 1);
    return (E) elements[pos];
  }

  // The main insertion and extraction methods are addFirst,
  // addLast, pollFirst, pollLast. The other methods are defined in
  // terms of these.

  /**
   * Inserts the specified element at the front of this deque.
   *
   * @param e the element to add
   * @throws NullPointerException if the specified element is null
   */
  @Override
  public void addFirst(E e) {
    if (e == null)
      throw new NullPointerException();
    head = (head - 1) & (elements.length - 1);
    elements[head] = e;
    if (head == tail)
      doubleCapacity();
  }

  /**
   * Inserts the specified element at the end of this deque.
   *
   * <p>This method is equivalent to {@link #add}.
   *
   * @param e the element to add
   * @throws NullPointerException if the specified element is null
   */
  @Override
  public void addLast(E e) {
    if (e == null)
      throw new NullPointerException();
    elements[tail] = e;
    if ( (tail = (tail + 1) & (elements.length - 1)) == head)
      doubleCapacity();
  }

  /**
   * Inserts the specified element at the front of this deque.
   *
   * @param e the element to add
   * @return {@code true} (as specified by {@link Deque#offerFirst})
   * @throws NullPointerException if the specified element is null
   */
  @Override
  public boolean offerFirst(E e) {
    addFirst(e);
    return true;
  }

  /**
   * Inserts the specified element at the end of this deque.
   *
   * @param e the element to add
   * @return {@code true} (as specified by {@link Deque#offerLast})
   * @throws NullPointerException if the specified element is null
   */
  @Override
  public boolean offerLast(E e) {
    addLast(e);
    return true;
  }

  /**
   * @throws NoSuchElementException {@inheritDoc}
   */
  @Override
  public E removeFirst() {
    E x = pollFirst();
    if (x == null)
      throw new NoSuchElementException();
    return x;
  }

  /**
   * @throws NoSuchElementException {@inheritDoc}
   */
  @Override
  public E removeLast() {
    E x = pollLast();
    if (x == null)
      throw new NoSuchElementException();
    return x;
  }

  @Override
  public E pollFirst() {
    int h = head;
    E result = (E) elements[h];
    // Element is null if deque empty
    if (result == null)
      return null;
    elements[h] = null;     // Must null out slot
    head = (h + 1) & (elements.length - 1);
    return result;
  }

  @Override
  public E pollLast() {
    int t = (tail - 1) & (elements.length - 1);
    E result = (E) elements[t];
    if (result == null)
      return null;
    elements[t] = null;
    tail = t;
    return result;
  }

  /**
   * @throws NoSuchElementException {@inheritDoc}
   */
  @Override
  public E getFirst() {
    E result = (E) elements[head];
    if (result == null)
      throw new NoSuchElementException();
    return result;
  }

  /**
   * @throws NoSuchElementException {@inheritDoc}
   */
  @Override
  public E getLast() {
    E result = (E) elements[(tail - 1) & (elements.length - 1)];
    if (result == null)
      throw new NoSuchElementException();
    return result;
  }

  @Override
  public E peekFirst() {
    // elements[head] is null if deque empty
    return (E) elements[head];
  }

  @Override
  public E peekLast() {
    return (E) elements[(tail - 1) & (elements.length - 1)];
  }

  /**
   * Removes the first occurrence of the specified element in this
   * deque (when traversing the deque from head to tail).
   * If the deque does not contain the element, it is unchanged.
   * More formally, removes the first element {@code e} such that
   * {@code o.equals(e)} (if such an element exists).
   * Returns {@code true} if this deque contained the specified element
   * (or equivalently, if this deque changed as a result of the call).
   *
   * @param o element to be removed from this deque, if present
   * @return {@code true} if the deque contained the specified element
   */
  @Override
  public boolean removeFirstOccurrence(Object o) {
    if (o == null)
      return false;
    int mask = elements.length - 1;
    int i = head;
    Object x;
    while ( (x = elements[i]) != null) {
      if (o.equals(x)) {
        delete(i);
        return true;
      }
      i = (i + 1) & mask;
    }
    return false;
  }

  /**
   * Removes the last occurrence of the specified element in this
   * deque (when traversing the deque from head to tail).
   * If the deque does not contain the element, it is unchanged.
   * More formally, removes the last element {@code e} such that
   * {@code o.equals(e)} (if such an element exists).
   * Returns {@code true} if this deque contained the specified element
   * (or equivalently, if this deque changed as a result of the call).
   *
   * @param o element to be removed from this deque, if present
   * @return {@code true} if the deque contained the specified element
   */
  @Override
  public boolean removeLastOccurrence(Object o) {
    if (o == null)
      return false;
    int mask = elements.length - 1;
    int i = (tail - 1) & mask;
    Object x;
    while ( (x = elements[i]) != null) {
      if (o.equals(x)) {
        delete(i);
        return true;
      }
      i = (i - 1) & mask;
    }
    return false;
  }

  // *** Queue methods ***

  /**
   * Inserts the specified element at the end of this deque.
   *
   * <p>This method is equivalent to {@link #addLast}.
   *
   * @param e the element to add
   * @return {@code true} (as specified by {@link Collection#add})
   * @throws NullPointerException if the specified element is null
   */
  @Override
  public boolean add(E e) {
    return offerLast(e);
  }

  /**
   * Inserts the specified element at the end of this deque.
   *
   * <p>This method is equivalent to {@link #offerLast}.
   *
   * @param e the element to add
   * @return {@code true} (as specified by {@link Queue#offer})
   * @throws NullPointerException if the specified element is null
   */
  @Override
  public boolean offer(E e) {
    return offerLast(e);
  }

  /**
   * Retrieves and removes the head of the queue represented by this deque.
   *
   * This method differs from {@link #poll poll} only in that it throws an
   * exception if this deque is empty.
   *
   * <p>This method is equivalent to {@link #removeFirst}.
   *
   * @return the head of the queue represented by this deque
   * @throws NoSuchElementException {@inheritDoc}
   */
  @Override
  public E remove() {
    return removeFirst();
  }

  /**
   * Retrieves and removes the head of the queue represented by this deque
   * (in other words, the first element of this deque), or returns
   * {@code null} if this deque is empty.
   *
   * <p>This method is equivalent to {@link #pollFirst}.
   *
   * @return the head of the queue represented by this deque, or
   *         {@code null} if this deque is empty
   */
  @Override
  public E poll() {
    return pollFirst();
  }

  /**
   * Retrieves, but does not remove, the head of the queue represented by
   * this deque.  This method differs from {@link #peek peek} only in
   * that it throws an exception if this deque is empty.
   *
   * <p>This method is equivalent to {@link #getFirst}.
   *
   * @return the head of the queue represented by this deque
   * @throws NoSuchElementException {@inheritDoc}
   */
  @Override
  public E element() {
    return getFirst();
  }

  /**
   * Retrieves, but does not remove, the head of the queue represented by
   * this deque, or returns {@code null} if this deque is empty.
   *
   * <p>This method is equivalent to {@link #peekFirst}.
   *
   * @return the head of the queue represented by this deque, or
   *         {@code null} if this deque is empty
   */
  @Override
  public E peek() {
    return peekFirst();
  }

  // *** Stack methods ***

  /**
   * Pushes an element onto the stack represented by this deque.  In other
   * words, inserts the element at the front of this deque.
   *
   * <p>This method is equivalent to {@link #addFirst}.
   *
   * @param e the element to push
   * @throws NullPointerException if the specified element is null
   */
  @Override
  public void push(E e) {
    addFirst(e);
  }

  /**
   * Pops an element from the stack represented by this deque.  In other
   * words, removes and returns the first element of this deque.
   *
   * <p>This method is equivalent to {@link #removeFirst()}.
   *
   * @return the element at the front of this deque (which is the top
   *         of the stack represented by this deque)
   * @throws NoSuchElementException {@inheritDoc}
   */
  @Override
  public E pop() {
    return removeFirst();
  }

  private void checkInvariants() {
    assert elements[tail] == null;
    assert head == tail ? elements[head] == null :
        (elements[head] != null &&
            elements[(tail - 1) & (elements.length - 1)] != null);
    assert elements[(head - 1) & (elements.length - 1)] == null;
  }

  /**
   * Removes the element at the specified position in the elements array,
   * adjusting head and tail as necessary.  This can result in motion of
   * elements backwards or forwards in the array.
   *
   * <p>This method is called delete rather than remove to emphasize
   * that its semantics differ from those of {@link List#remove(int)}.
   *
   * @return true if elements moved backwards
   */
  private boolean delete(int i) {
    checkInvariants();
    final Object[] localElements = this.elements;
    final int mask = localElements.length - 1;
    final int h = head;
    final int t = tail;
    final int front = (i - h) & mask;
    final int back  = (t - i) & mask;

    // Invariant: head <= i < tail mod circularity
    if (front >= ((t - h) & mask))
      throw new ConcurrentModificationException();

    // Optimize for least element motion
    if (front < back) {
      if (h <= i) {
        System.arraycopy(localElements, h, localElements, h + 1, front);
      } else { // Wrap around
        System.arraycopy(localElements, 0, localElements, 1, i);
        localElements[0] = localElements[mask];
        System.arraycopy(localElements, h, localElements, h + 1, mask - h);
      }
      localElements[h] = null;
      head = (h + 1) & mask;
      return false;
    } else {
      if (i < t) { // Copy the null tail as well
        System.arraycopy(localElements, i + 1, localElements, i, back);
        tail = t - 1;
      } else { // Wrap around
        System.arraycopy(localElements, i + 1, localElements, i, mask - i);
        localElements[mask] = localElements[0];
        System.arraycopy(localElements, 1, localElements, 0, t);
        tail = (t - 1) & mask;
      }
      return true;
    }
  }

  // *** Collection Methods ***

  /**
   * Returns the number of elements in this deque.
   *
   * @return the number of elements in this deque
   */
  @Override
  public int size() {
    return (tail - head) & (elements.length - 1);
  }

  /**
   * Returns {@code true} if this deque contains no elements.
   *
   * @return {@code true} if this deque contains no elements
   */
  @Override
  public boolean isEmpty() {
    return head == tail;
  }

  /**
   * Returns an iterator over the elements in this deque.  The elements
   * will be ordered from first (head) to last (tail).  This is the same
   * order that elements would be dequeued (via successive calls to
   * {@link #remove} or popped (via successive calls to {@link #pop}).
   *
   * @return an iterator over the elements in this deque
   */
  @Override
  public Iterator<E> iterator() {
    return new DeqIterator();
  }

  @Override
  public Iterator<E> descendingIterator() {
    return new DescendingIterator();
  }

  private class DeqIterator implements Iterator<E> {
    /**
     * Index of element to be returned by subsequent call to next.
     */
    private int cursor = head;

    /**
     * Tail recorded at construction (also in remove), to stop
     * iterator and also to check for comodification.
     */
    private int fence = tail;

    /**
     * Index of element returned by most recent call to next.
     * Reset to -1 if element is deleted by a call to remove.
     */
    private int lastRet = -1;

    @Override
    public boolean hasNext() {
      return cursor != fence;
    }

    @Override
    public E next() {
      if (cursor == fence)
        throw new NoSuchElementException();
      E result = (E) elements[cursor];
      // This check doesn't catch all possible comodifications,
      // but does catch the ones that corrupt traversal
      if (tail != fence || result == null)
        throw new ConcurrentModificationException();
      lastRet = cursor;
      cursor = (cursor + 1) & (elements.length - 1);
      return result;
    }

    @Override
    public void remove() {
      if (lastRet < 0)
        throw new IllegalStateException();
      if (delete(lastRet)) { // if left-shifted, undo increment in next()
        cursor = (cursor - 1) & (elements.length - 1);
        fence = tail;
      }
      lastRet = -1;
    }

    @Override
    public void forEachRemaining(Consumer<? super E> action) {
      Objects.requireNonNull(action);
      Object[] a = elements;
      int m = a.length - 1;
      int f = fence;
      int i = cursor;
      cursor = f;
      while (i != f) {
        E e = (E)a[i];
        i = (i + 1) & m;
        if (e == null)
          throw new ConcurrentModificationException();
        action.accept(e);
      }
    }
  }

  private class DescendingIterator implements Iterator<E> {
    /*
     * This class is nearly a mirror-image of DeqIterator, using
     * tail instead of head for initial cursor, and head instead of
     * tail for fence.
     */
    private int cursor = tail;
    private int fence = head;
    private int lastRet = -1;

    @Override
    public boolean hasNext() {
      return cursor != fence;
    }

    @Override
    public E next() {
      if (cursor == fence)
        throw new NoSuchElementException();
      cursor = (cursor - 1) & (elements.length - 1);
      E result = (E) elements[cursor];
      if (head != fence || result == null)
        throw new ConcurrentModificationException();
      lastRet = cursor;
      return result;
    }

    @Override
    public void remove() {
      if (lastRet < 0)
        throw new IllegalStateException();
      if (!delete(lastRet)) {
        cursor = (cursor + 1) & (elements.length - 1);
        fence = head;
      }
      lastRet = -1;
    }
  }

  /**
   * Returns {@code true} if this deque contains the specified element.
   * More formally, returns {@code true} if and only if this deque contains
   * at least one element {@code e} such that {@code o.equals(e)}.
   *
   * @param o object to be checked for containment in this deque
   * @return {@code true} if this deque contains the specified element
   */
  @Override
  public boolean contains(Object o) {
    if (o == null)
      return false;
    int mask = elements.length - 1;
    int i = head;
    Object x;
    while ( (x = elements[i]) != null) {
      if (o.equals(x))
        return true;
      i = (i + 1) & mask;
    }
    return false;
  }

  /**
   * Removes a single instance of the specified element from this deque.
   * If the deque does not contain the element, it is unchanged.
   * More formally, removes the first element {@code e} such that
   * {@code o.equals(e)} (if such an element exists).
   * Returns {@code true} if this deque contained the specified element
   * (or equivalently, if this deque changed as a result of the call).
   *
   * <p>This method is equivalent to {@link #removeFirstOccurrence(Object)}.
   *
   * @param o element to be removed from this deque, if present
   * @return {@code true} if this deque contained the specified element
   */
  @Override
  public boolean remove(Object o) {
    return removeFirstOccurrence(o);
  }

  /**
   * Removes all of the elements from this deque.
   * The deque will be empty after this call returns.
   */
  @Override
  public void clear() {
    int h = head;
    int t = tail;
    if (h != t) { // clear all cells
      head = tail = 0;
      int i = h;
      int mask = elements.length - 1;
      do {
        elements[i] = null;
        i = (i + 1) & mask;
      } while (i != t);
    }
  }

  /**
   * Returns an array containing all of the elements in this deque
   * in proper sequence (from first to last element).
   *
   * <p>The returned array will be "safe" in that no references to it are
   * maintained by this deque.  (In other words, this method must allocate
   * a new array).  The caller is thus free to modify the returned array.
   *
   * <p>This method acts as bridge between array-based and collection-based
   * APIs.
   *
   * @return an array containing all of the elements in this deque
   */
  @Override
  public Object[] toArray() {
    return copyElements(new Object[size()]);
  }

  /**
   * Returns an array containing all of the elements in this deque in
   * proper sequence (from first to last element); the runtime type of the
   * returned array is that of the specified array.  If the deque fits in
   * the specified array, it is returned therein.  Otherwise, a new array
   * is allocated with the runtime type of the specified array and the
   * size of this deque.
   *
   * <p>If this deque fits in the specified array with room to spare
   * (i.e., the array has more elements than this deque), the element in
   * the array immediately following the end of the deque is set to
   * {@code null}.
   *
   * <p>Like the {@link #toArray()} method, this method acts as bridge between
   * array-based and collection-based APIs.  Further, this method allows
   * precise control over the runtime type of the output array, and may,
   * under certain circumstances, be used to save allocation costs.
   *
   * <p>Suppose {@code x} is a deque known to contain only strings.
   * The following code can be used to dump the deque into a newly
   * allocated array of {@code String}:
   *
   *  <pre> {@code String[] y = x.toArray(new String[0]);}</pre>
   *
   * Note that {@code toArray(new Object[0])} is identical in function to
   * {@code toArray()}.
   *
   * @param a the array into which the elements of the deque are to
   *          be stored, if it is big enough; otherwise, a new array of the
   *          same runtime type is allocated for this purpose
   * @return an array containing all of the elements in this deque
   * @throws ArrayStoreException if the runtime type of the specified array
   *         is not a supertype of the runtime type of every element in
   *         this deque
   * @throws NullPointerException if the specified array is null
   */
  @Override
  public <T> T[] toArray(T[] a) {
    int size = size();
    if (a.length < size)
      a = (T[])java.lang.reflect.Array.newInstance(
          a.getClass().getComponentType(), size);
    copyElements(a);
    if (a.length > size)
      a[size] = null;
    return a;
  }

  // *** Object methods ***

  /**
   * Creates a <em><a href="Spliterator.html#binding">late-binding</a></em>
   * and <em>fail-fast</em> {@link Spliterator} over the elements in this
   * deque.
   *
   * <p>The {@code Spliterator} reports {@link Spliterator#SIZED},
   * {@link Spliterator#SUBSIZED}, {@link Spliterator#ORDERED}, and
   * {@link Spliterator#NONNULL}.  Overriding implementations should document
   * the reporting of additional characteristic values.
   *
   * @return a {@code Spliterator} over the elements in this deque
   * @since 1.8
   */
  @Override
  public Spliterator<E> spliterator() {
    return new DeqSpliterator<>(this, -1, -1);
  }

  static final class DeqSpliterator<E> implements Spliterator<E> {
    private final RandomAccessArrayDeque<E> deq;
    private int fence;  // -1 until first use
    private int index;  // current index, modified on traverse/split

    /** Creates new spliterator covering the given array and range */
    DeqSpliterator(RandomAccessArrayDeque<E> deq, int origin, int fence) {
      this.deq = deq;
      this.index = origin;
      this.fence = fence;
    }

    private int getFence() { // force initialization
      int t;
      if ((t = fence) < 0) {
        t = fence = deq.tail;
        index = deq.head;
      }
      return t;
    }

    @Override
    public DeqSpliterator<E> trySplit() {
      int t = getFence();
      int h = index;
      int n = deq.elements.length;
      if (h != t && ((h + 1) & (n - 1)) != t) {
        if (h > t)
          t += n;
        index = ((h + t) >>> 1) & (n - 1);
        return new DeqSpliterator<>(deq, h, index);
      }
      return null;
    }

    @Override
    public void forEachRemaining(Consumer<? super E> consumer) {
      if (consumer == null)
        throw new NullPointerException();
      Object[] a = deq.elements;
      int m = a.length - 1;
      int f = getFence();
      int i = index;
      index = f;
      while (i != f) {
        E e = (E)a[i];
        i = (i + 1) & m;
        if (e == null)
          throw new ConcurrentModificationException();
        consumer.accept(e);
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super E> consumer) {
      if (consumer == null)
        throw new NullPointerException();
      Object[] a = deq.elements;
      int m = a.length - 1;
      getFence();
      int i = index;
      if (i != fence) {
        E e = (E)a[i];
        index = (i + 1) & m;
        if (e == null)
          throw new ConcurrentModificationException();
        consumer.accept(e);
        return true;
      }
      return false;
    }

    @Override
    public long estimateSize() {
      int n = getFence() - index;
      if (n < 0)
        n += deq.elements.length;
      return n;
    }

    @Override
    public int characteristics() {
      return Spliterator.ORDERED | Spliterator.SIZED |
          Spliterator.NONNULL | Spliterator.SUBSIZED;
    }
  }

}
