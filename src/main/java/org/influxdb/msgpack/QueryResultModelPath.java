package org.influxdb.msgpack;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple object model path, used internally for navigating on QueryResult objects
 * when traverse and parse the MessagePack data.
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
class QueryResultModelPath {
  private List<String> names = new ArrayList<>();
  private List<Object> objects = new ArrayList<>();
  private int lastIndex = -1;

  public void add(final String name, final Object object) {
    names.add(name);
    objects.add(object);
    lastIndex++;
  }

  public <T> T getLastObject() {
    return (T) objects.get(lastIndex);
  }

  public void removeLast() {
    names.remove(lastIndex);
    objects.remove(lastIndex);
    lastIndex--;
  }

  public boolean compareEndingPath(final String... names) {
    int diff = (lastIndex + 1) - names.length;
    if (diff < 0) {
      return false;
    }
    for (int i = 0; i < names.length; i++) {
      if (!names[i].equals(this.names.get(i + diff))) {
        return false;
      }
    }

    return true;
  }
}
