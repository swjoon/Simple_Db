package simpleDb;

import sql.Sql;

public interface SimpleDb {

    void run(String sql, Object... params);

    Sql genSql();
}
