package org.influxdb.querybuilder;

import org.influxdb.dto.Query;
import org.influxdb.querybuilder.clauses.*;

import java.util.Arrays;
import java.util.List;

import static org.influxdb.querybuilder.Operations.*;

public abstract class BuiltQuery extends Query {

    public BuiltQuery(String database) {
        super(null, database);
    }

    public BuiltQuery(String database, boolean requiresPost) {
        super(null, database, requiresPost);
    }

    abstract StringBuilder buildQueryString();

    static StringBuilder addSemicolonIfNeeded(StringBuilder stringBuilder) {
        int length = moveToEndOfText(stringBuilder);
        if (length == 0 || stringBuilder.charAt(length - 1) != ';')
            stringBuilder.append(';');
        return stringBuilder;
    }

    private static int moveToEndOfText(StringBuilder stringBuilder) {
        int length = stringBuilder.length();
        while (length > 0 && stringBuilder.charAt(length - 1) <= ' ')
            length -= 1;
        if (length != stringBuilder.length())
            stringBuilder.setLength(length);
        return length;
    }

    @Override
    public String getCommand() {
        StringBuilder sb = buildQueryString();
        addSemicolonIfNeeded(sb);
        return sb.toString();
    }

    @Override
    public String getCommandWithUrlEncoded() {
        return encode(getCommand());
    }

    /**
     * The query builder shall provide all the building blocks needed, only a static block shall be used
     */
    public static final class QueryBuilder {

        private QueryBuilder() {
        }

        public static Select.Builder select(String... columns) {
            return select((Object[]) columns);
        }

        public static Select.Builder select(Object... columns) {
            return new Select.Builder(Arrays.asList(columns));
        }

        public static Selection select() {
            return new Selection();
        }

        public static Clause eq(String name, Object value) {
            return new SimpleClause(name, EQ, value);
        }

        public static Clause ne(String name, Object value) {
            return new SimpleClause(name, NE, value);
        }

        public static Clause contains(String name, String value) {
            return new ContainsClause(name, value);
        }

        public static Clause regex(String name, String value) {
            return new RegexClause(name, value);
        }

        public static Clause nregex(String name, String value) {
            return new NegativeRegexClause(name, value);
        }

        public static Clause lt(String name, Object value) {
            return new SimpleClause(name, LT, value);
        }

        public static Clause lte(String name, Object value) {
            return new SimpleClause(name, LTE, value);
        }

        public static Clause gt(String name, Object value) {
            return new SimpleClause(name, GT, value);
        }

        public static Clause gte(String name, Object value) {
            return new SimpleClause(name, GTE, value);
        }

        /**
         * @return
         */
        public static Ordering asc() {
            return new Ordering(false);
        }

        /**
         * InfluxDB supports only time for ordering
         *
         * @return
         */
        public static Ordering desc() {
            return new Ordering(true);
        }

        public static Object raw(String str) {
            return new RawString(str);
        }

        public static Object column(String name) {
            return FunctionFactory.column(name);
        }

        /**
         * Functions
         */

        public static Object now() {
            return FunctionFactory.now();
        }

    }

}
