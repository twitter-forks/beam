package org.apache.beam.sdk.util.common;

import java.util.List;

/** {@link Reiterator} that wraps a {@link List}. */
public class ListReiterator<T> implements Reiterator<T> {
  private List<T> list;
  private int index;

  public ListReiterator(List<T> list) {
    this(list, 0);
  }

  private ListReiterator(List<T> list, int index) {
    this.list = list;
    this.index = index;
  }

  @Override
  public T next() {
    return list.get(index++);
  }

  @Override
  public boolean hasNext() {
    return index < list.size();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reiterator<T> copy() {
    return new ListReiterator<T>(list, index);
  }
}
