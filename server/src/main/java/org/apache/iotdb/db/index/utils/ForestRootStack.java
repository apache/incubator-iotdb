package org.apache.iotdb.db.index.utils;

import java.util.Stack;
import org.apache.iotdb.tsfile.utils.Pair;

public class ForestRootStack<T> {

  private Stack<T> rootNodes = null;

  public ForestRootStack() {
    this.rootNodes = new Stack<T>();
  }

  public void push(T aRootNode) {
    if (rootNodes == null) {
      rootNodes = new Stack<T>();
    }

    rootNodes.push(aRootNode);
  }

  public Pair<T, T> popPair() {
    if (rootNodes == null) {
      return null;
    }

    T rightChild = rootNodes.pop();
    T leftChild = rootNodes.pop();
    return new Pair<>(leftChild, rightChild);
  }

  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (T t : rootNodes) {
      stringBuilder.append(t).append(",");
    }
    return stringBuilder.toString();
  }

  public int size() {
    return this.rootNodes.size();
  }

  public T get(int index) {
    return this.rootNodes.get(index);
  }
}