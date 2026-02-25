package com.amazonaws.athena.connectors.mysql;

import org.apache.calcite.sql.SqlDialect;

public class MysqlSqlDialect extends SqlDialect
{
    private final boolean catalogCasingFilter;

    public static final SqlDialect DEFAULT = org.apache.calcite.sql.dialect.MysqlSqlDialect.DEFAULT;

    private MysqlSqlDialect(Context context, boolean catalogCasingFilter)
    {
        super(context);
        this.catalogCasingFilter = catalogCasingFilter;
    }

    public MysqlSqlDialect(boolean catalogCasingFilter)
    {
        this(EMPTY_CONTEXT
                .withDatabaseProduct(DatabaseProduct.MYSQL)
                .withIdentifierQuoteString("`"), catalogCasingFilter);
    }

    @Override
    public StringBuilder quoteIdentifier(StringBuilder buf, String identifier)
    {
        if (catalogCasingFilter) {
            // Backtick required for case-sensitive tables
            return buf.append("`").append(identifier.toUpperCase()).append("`");
        }
        else {
            return super.quoteIdentifier(buf, identifier);
        }
    }
}
