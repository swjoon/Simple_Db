package simpleDb;

import sql.Sql;

import java.sql.ResultSet;
import java.util.List;

public interface SimpleDb {

    <T> T run(String sql, Object... params);

    <T> T executeQuery(String sql, Class<T> type, Object... params);

    void close();

    Sql genSql();;

    <T> List<T> selectRows(String sql, Class<?> cls, Object... params);

    <T> T selectRow(String sql, Class<?> cls, Object... params);

    void startTransaction();

    void rollback();

    void commit();
}
