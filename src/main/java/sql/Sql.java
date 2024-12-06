package sql;

import entity.Article;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface Sql {
    SqlImpl append(String sql);

    SqlImpl appendIn(String sql, Object... param);

    int insert();

    int update();

    int delete();

    List<Map<String, Object>> selectRows();

    Map<String, Object> selectRow();

    LocalDateTime selectDatetime();

    Long selectLong();

    String selectString();

    Boolean selectBoolean();

    List<Long> selectLongs();

    <T> List<T> selectRows(Class<T> type);

    <T> T selectRow(Class<T> type);
}
