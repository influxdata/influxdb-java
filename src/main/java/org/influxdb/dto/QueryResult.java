package org.influxdb.dto;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

/**
 * {Purpose of This Type}
 *
 * {Other Notes Relating to This Type (Optional)}
 *
 * @author stefan
 *
 */
// {
// "results": [
// {
// "series": [{}],
// "error": "...."
// }
// ],
// "error": "...."
// }

// {"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-06-06T14:55:27.195Z",90],["2015-06-06T14:56:24.556Z",90]]}]}]}
// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["mydb"]]}]}]}
public class QueryResult {

    private List<Result> results;
    private String error;

    /**
     * @return the results
     */
    public List<Result> getResults() {
        return this.results;
    }

    /**
     * @param results
     *            the results to set
     */
    public void setResults(final List<Result> results) {
        this.results = results;
    }

    /**
     * @return the error
     */
    public String getError() {
        return this.error;
    }

    /**
     * @param error
     *            the error to set
     */
    public void setError(final String error) {
        this.error = error;
    }

    public static class Result {
        private List<Series> series;
        private String error;

        /**
         * @return the series
         */
        public List<Series> getSeries() {
            return this.series;
        }

        /**
         * @param series
         *            the series to set
         */
        public void setSeries(final List<Series> series) {
            this.series = series;
        }

        /**
         * @return the error
         */
        public String getError() {
            return this.error;
        }

        /**
         * @param error
         *            the error to set
         */
        public void setError(final String error) {
            this.error = error;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Result [series=");
            builder.append(this.series);
            builder.append(", error=");
            builder.append(this.error);
            builder.append("]");
            return builder.toString();
        }

    }

    public static class Series {
        private String name;
        private List<String> columns;
        private List<List<Object>> values;
        private Map<String, String> tags = Maps.newTreeMap(Ordering.natural());

        /**
         * @return the name
         */
        public String getName() {
            return this.name;
        }

        /**
         * @param name
         *            the name to set
         */
        public void setName(final String name) {
            this.name = name;
        }

        /**
         * @return the columns
         */
        public List<String> getColumns() {
            return this.columns;
        }

        /**
         * @param columns
         *            the columns to set
         */
        public void setColumns(final List<String> columns) {
            this.columns = columns;
        }

        /**
         * @return the values
         */
        public List<List<Object>> getValues() {
            return this.values;
        }

        /**
         * @param values
         *            the values to set
         */
        public void setValues(final List<List<Object>> values) {
            this.values = values;
        }

        /**
         * @param values
         *            the values to set
         */
        public void setTags(final Map<String, String> tags) {
            this.tags = tags;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Series [name=");
            builder.append(this.name);
            builder.append(", tags=");
            builder.append(this.tags);
            builder.append(", columns=");
            builder.append(this.columns);
            builder.append(", values=");
            builder.append(this.values);
            builder.append("]");
            return builder.toString();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("QueryResult [results=");
        builder.append(this.results);
        builder.append(", error=");
        builder.append(this.error);
        builder.append("]");
        return builder.toString();
    }

}
