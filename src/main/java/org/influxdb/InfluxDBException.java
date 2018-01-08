package org.influxdb;

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

  static final String FIELD_TYPE_CONFLICT_ERROR = "field type conflict";
  static final String POINTS_BEYOND_RETENTION_POLICY_ERROR = "points beyond retention policy";
  static final String UNABLE_TO_PARSE_ERROR = "unable to parse";
  static final String HINTED_HAND_OFF_QUEUE_NOT_EMPTY_ERROR = "hinted handoff queue not empty";
  static final String DATABASE_NOT_FOUND_ERROR = "database not found";
  static final String CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR = "cache-max-memory-size exceeded";

  public static final class DatabaseNotFoundError extends InfluxDBException {
    private DatabaseNotFoundError(final String message) {
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

  public static InfluxDBException buildExceptionForErrorState(final String error) {
    if (error.contains(DATABASE_NOT_FOUND_ERROR)) {
      return new DatabaseNotFoundError(error);
    }
    if (error.contains(POINTS_BEYOND_RETENTION_POLICY_ERROR)) {
      return new PointsBeyondRetentionPolicyException(error);
    }
    if (error.contains(FIELD_TYPE_CONFLICT_ERROR)) {
      return new FieldTypeConflictException(error);
    }
    if (error.contains(UNABLE_TO_PARSE_ERROR)) {
      return new UnableToParseException(error);
    }
    if (error.contains(HINTED_HAND_OFF_QUEUE_NOT_EMPTY_ERROR)) {
      return new HintedHandOffQueueNotEmptyException(error);
    }
    if (error.contains(CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR)) {
      return new CacheMaxMemorySizeExceededException(error);
    }
    throw new InfluxDBException(error);
  }

}
