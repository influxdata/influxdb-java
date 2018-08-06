package org.influxdb;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;

/**
 * A wrapper for various exceptions caused while interacting with InfluxDB.
 *
 * @author Simon Legner
 */
public class InfluxDBException extends RuntimeException {

  public InfluxDBException(final String message) {
    super(message);
  }

  public InfluxDBException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public InfluxDBException(final Throwable cause) {
    super(cause);
  }

  /**
   * @return true if the operation may succeed if repeated, false otherwise.
   */
  public boolean isRetryWorth() {
    return true;
  }

  /* See https://github.com/influxdata/influxdb/blob/master/tsdb/shard.go */
  static final String FIELD_TYPE_CONFLICT_ERROR = "field type conflict";
  /* See https://github.com/influxdata/influxdb/blob/master/coordinator/points_writer.go */
  static final String POINTS_BEYOND_RETENTION_POLICY_ERROR = "points beyond retention policy";
  /* See https://github.com/influxdata/influxdb/blob/master/models/points.go */
  static final String UNABLE_TO_PARSE_ERROR = "unable to parse";
  /* See https://github.com/influxdata/telegraf/blob/master/plugins/outputs/influxdb/influxdb.go */
  static final String HINTED_HAND_OFF_QUEUE_NOT_EMPTY_ERROR = "hinted handoff queue not empty";
  /* See https://github.com/influxdata/influxdb/blob/master/tsdb/engine/tsm1/cache.go */
  static final String CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR = "cache-max-memory-size exceeded";
  /* For all messages below see https://github.com/influxdata/influxdb/blob/master/services/httpd/handler.go */
  static final String DATABASE_NOT_FOUND_ERROR = "database not found";
  static final String USER_REQUIRED_ERROR = "user is required to write to database";
  static final String USER_NOT_AUTHORIZED_ERROR = "user is not authorized to write to database";
  static final String AUTHORIZATION_FAILED_ERROR = "authorization failed";
  static final String USERNAME_REQUIRED_ERROR = "username required";

  public static final class DatabaseNotFoundException extends InfluxDBException {
    private DatabaseNotFoundException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return false;
    }
  }

  public static final class HintedHandOffQueueNotEmptyException extends InfluxDBException {
    private HintedHandOffQueueNotEmptyException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return false;
    }
  }

  public static final class UnableToParseException extends InfluxDBException {
    private UnableToParseException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return false;
    }
  }

  public static final class FieldTypeConflictException extends InfluxDBException {
    private FieldTypeConflictException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return false;
    }
  }

  public static final class PointsBeyondRetentionPolicyException extends InfluxDBException {
    private PointsBeyondRetentionPolicyException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return false;
    }
  }

  public static final class CacheMaxMemorySizeExceededException extends InfluxDBException {
    private CacheMaxMemorySizeExceededException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return true;
    }
  }

  public static final class RetryBufferOverrunException extends InfluxDBException {
    public RetryBufferOverrunException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return false;
    }
  }

  public static final class AuthorizationFailedException extends InfluxDBException {
    public AuthorizationFailedException(final String message) {
      super(message);
    }

    public boolean isRetryWorth() {
      return false;
    }
  }

  public static InfluxDBException buildExceptionFromErrorMessage(final String errorMessage) {
    if (errorMessage.contains(DATABASE_NOT_FOUND_ERROR)) {
      return new DatabaseNotFoundException(errorMessage);
    }
    if (errorMessage.contains(POINTS_BEYOND_RETENTION_POLICY_ERROR)) {
      return new PointsBeyondRetentionPolicyException(errorMessage);
    }
    if (errorMessage.contains(FIELD_TYPE_CONFLICT_ERROR)) {
      return new FieldTypeConflictException(errorMessage);
    }
    if (errorMessage.contains(UNABLE_TO_PARSE_ERROR)) {
      return new UnableToParseException(errorMessage);
    }
    if (errorMessage.contains(HINTED_HAND_OFF_QUEUE_NOT_EMPTY_ERROR)) {
      return new HintedHandOffQueueNotEmptyException(errorMessage);
    }
    if (errorMessage.contains(CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR)) {
      return new CacheMaxMemorySizeExceededException(errorMessage);
    }
    if (errorMessage.contains(USER_REQUIRED_ERROR)
            || errorMessage.contains(USER_NOT_AUTHORIZED_ERROR)
            || errorMessage.contains(AUTHORIZATION_FAILED_ERROR)
            || errorMessage.contains(USERNAME_REQUIRED_ERROR)) {
      return new AuthorizationFailedException(errorMessage);
    }
    return new InfluxDBException(errorMessage);
  }

  private static class ErrorMessage {
    public String error;
  }

  public static InfluxDBException buildExceptionForErrorState(final String errorBody) {
    try {
      Moshi moshi = new Moshi.Builder().build();
      JsonAdapter<ErrorMessage> adapter = moshi.adapter(ErrorMessage.class).lenient();
      ErrorMessage errorMessage = adapter.fromJson(errorBody);
      return InfluxDBException.buildExceptionFromErrorMessage(errorMessage.error);
    } catch (Exception e) {
      return new InfluxDBException(errorBody);
    }
  }
}
