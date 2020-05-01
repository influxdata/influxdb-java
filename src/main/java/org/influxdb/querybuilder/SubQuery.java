package org.influxdb.querybuilder;

public abstract class SubQuery<T extends WithSubquery> implements QueryStringBuilder {

  private T parent;

  void setParent(final T parent) {
    this.parent = parent;
  }

  T getParent() {
    return parent;
  }

  public T close() {
    parent.setSubQuery(this);
    return parent;
  }
}
