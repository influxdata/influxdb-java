package org.influxdb.msgpack;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.msgpack.core.ExtensionTypeHeader;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

/**
 * Traverse the MessagePack input stream and return Query Result object(s).
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
public class MessagePackTraverser {

  private static final byte MSG_PACK_TIME_EXT_TYPE = 5;
  private String lastStringNode;

  /**
   * Traverse over the whole message pack stream.
   * This method can be used for converting query results in chunk.
   *
   * @param is
   *          The MessagePack format input stream
   * @return an Iterable over the QueryResult objects
   *
   */
  public Iterable<QueryResult> traverse(final InputStream is) {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(is);

    return () -> {
      return new Iterator<QueryResult>() {
        @Override
        public boolean hasNext() {
          try {
            return unpacker.hasNext();
          } catch (IOException e) {
            throw new InfluxDBException(e);
          }
        }

        @Override
        public QueryResult next() {
          return parse(unpacker);
        }
      };
    };

  }

  /**
   * Parse the message pack stream.
   * This method can be used for converting query
   * result from normal query response where exactly one QueryResult returned
   *
   * @param is
   *          The MessagePack format input stream
   * @return QueryResult
   *
   */
  public QueryResult parse(final InputStream is) {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(is);
    return parse(unpacker);
  }

  private QueryResult parse(final MessageUnpacker unpacker) {
    QueryResult queryResult = new QueryResult();
    QueryResultModelPath queryResultPath = new QueryResultModelPath();
    queryResultPath.add("queryResult", queryResult);
    try {
      traverse(unpacker, queryResultPath, 1);
    } catch (IOException e) {
      throw new InfluxDBException(e);
    }
    return queryResult;
  }

  void traverse(final MessageUnpacker unpacker, final QueryResultModelPath queryResultPath, final int readAmount)
      throws IOException {
    int amount = 0;

    while (unpacker.hasNext() && amount < readAmount) {
      MessageFormat format = unpacker.getNextFormat();
      ValueType type = format.getValueType();
      int length;
      ExtensionTypeHeader extension;
      Object o = null;
      byte[] dst;
      String addedName = null;
      Object addedObject = null;
      switch (type) {
      case NIL:
        unpacker.unpackNil();
        break;
      case BOOLEAN:
        o = unpacker.unpackBoolean();
        break;
      case INTEGER:
        switch (format) {
        case UINT64:
          o = unpacker.unpackBigInteger();
          break;
        case INT64:
        case UINT32:
          o = unpacker.unpackLong();
          break;
        default:
          o = unpacker.unpackInt();
          break;
        }
        break;
      case FLOAT:
        o = unpacker.unpackDouble();
        break;
      case STRING:
        o = unpacker.unpackString();
        lastStringNode = (String) o;
        if ("name".equals(o) && queryResultPath.compareEndingPath("series")) {
          queryResultPath.add("name", null);
        } else if (queryResultPath.compareEndingPath("name")) {
          queryResultPath.removeLast();
          Series series = queryResultPath.getLastObject();
          series.setName((String) o);
        } else if (queryResultPath.compareEndingPath("tags")) {
          queryResultPath.add("tagKey", o);
        } else if (queryResultPath.compareEndingPath("tagKey")) {
          String tagKey = queryResultPath.getLastObject();
          queryResultPath.removeLast();
          Map<String, String> tags = queryResultPath.getLastObject();
          tags.put(tagKey, (String) o);
        } else if (queryResultPath.compareEndingPath("columns")) {
          List<String> columns = queryResultPath.getLastObject();
          columns.add((String) o);
        }
        break;
      case BINARY:
        length = unpacker.unpackBinaryHeader();
        dst = new byte[length];
        unpacker.readPayload(dst);
        break;
      case ARRAY:
        length = unpacker.unpackArrayHeader();
        if (length > 0) {
          if ("results".equals(lastStringNode)) {
            QueryResult queryResult = queryResultPath.getLastObject();
            List<Result> results = new ArrayList<>();
            queryResult.setResults(results);
            addedName = "results";
            addedObject = results;
          } else if ("series".equals(lastStringNode) && queryResultPath.compareEndingPath("result")) {
            Result result = queryResultPath.getLastObject();
            List<Series> series = new ArrayList<>();
            result.setSeries(series);
            addedName = "seriesList";
            addedObject = series;
          } else if ("columns".equals(lastStringNode) && queryResultPath.compareEndingPath("series")) {
            Series series = queryResultPath.getLastObject();
            List<String> columns = new ArrayList<>();
            series.setColumns(columns);
            addedName = "columns";
            addedObject = columns;
          } else if ("values".equals(lastStringNode) && queryResultPath.compareEndingPath("series")) {
            Series series = queryResultPath.getLastObject();
            List<List<Object>> values = new ArrayList<>();
            series.setValues(values);
            addedName = "values";
            addedObject = values;
          } else if (queryResultPath.compareEndingPath("values")) {
            List<List<Object>> values = queryResultPath.getLastObject();
            List<Object> value = new ArrayList<>();
            values.add(value);
            addedName = "value";
            addedObject = value;
          }

          if (addedName != null) {
            queryResultPath.add(addedName, addedObject);
          }
          traverse(unpacker, queryResultPath, length);
          if (addedName != null) {
            queryResultPath.removeLast();
          }
        }
        break;
      case MAP:
        length = unpacker.unpackMapHeader();
        if (queryResultPath.compareEndingPath("results")) {
          List<Result> results = queryResultPath.getLastObject();
          Result result = new Result();
          results.add(result);
          addedName = "result";
          addedObject = result;
        } else if (queryResultPath.compareEndingPath("seriesList")) {
          List<Series> series = queryResultPath.getLastObject();
          Series s = new Series();
          series.add(s);
          addedName = "series";
          addedObject = s;
        } else if ("tags".equals(lastStringNode) && queryResultPath.compareEndingPath("series")) {
          Series series = queryResultPath.getLastObject();
          Map<String, String> tags = new HashMap<>();
          series.setTags(tags);
          addedName = "tags";
          addedObject = tags;
        }

        if (addedName != null) {
          queryResultPath.add(addedName, addedObject);
        }
        for (int i = 0; i < length; i++) {
          traverse(unpacker, queryResultPath, 1); // key
          traverse(unpacker, queryResultPath, 1); // value
        }
        if (addedName != null) {
          queryResultPath.removeLast();
        }
        break;
      case EXTENSION:
        final int nanosStartIndex = 8;
        extension = unpacker.unpackExtensionTypeHeader();
        if (extension.getType() == MSG_PACK_TIME_EXT_TYPE) {
          //decode epoch nanos in accordance with https://github.com/tinylib/msgp/blob/master/msgp/write.go#L594

          dst = new byte[extension.getLength()];
          unpacker.readPayload(dst);
          ByteBuffer bf = ByteBuffer.wrap(dst, 0, extension.getLength());
          long epochSeconds = bf.getLong();
          int nanosOffset = bf.getInt(nanosStartIndex);
          o = TimeUnit.SECONDS.toNanos(epochSeconds) + nanosOffset;
        }
        break;

      default:
      }

      if (queryResultPath.compareEndingPath("value")) {
        List<Object> value = queryResultPath.getLastObject();
        value.add(o);
      }
      amount++;
    }
  }
}
