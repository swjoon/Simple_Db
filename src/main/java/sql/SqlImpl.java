package sql;

import lombok.RequiredArgsConstructor;
import simpleDb.SimpleDb;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
public class SqlImpl implements Sql {
    private final SimpleDb simpleDb;
    private final StringBuilder query;
    private final List<Object> params;

    public SqlImpl(SimpleDb simpleDb) {
        this.simpleDb = simpleDb;
        this.query = new StringBuilder();
        this.params = new ArrayList<>();
    }

    public SqlImpl append(String sql) {
        query.append(sql).append(" ");
        return this;
    }

    public SqlImpl append(String sql, Object... param) {
        query.append(sql).append(" ");
        params.addAll(Arrays.asList(param));
        return this;
    }

    public SqlImpl appendIn(String sql, Object... param) {
        String inClause = IntStream
                .range(0, param.length)
                .mapToObj(i -> "?")
                .collect(Collectors.joining(", "));
        return append(sql.replace("?", inClause), param);
    }

    public int insert() {
        return simpleDb.run(query.toString(), params.toArray());
    }

    public int update() {
        return simpleDb.run(query.toString(), params.toArray());
    }

    public int delete() {
        return simpleDb.run(query.toString(), params.toArray());
    }

    public List<Map<String, Object>> selectRows() {
        return simpleDb.executeQuery(query.toString(), List.class, params.toArray());
    }

    public Map<String, Object> selectRow() {
        return simpleDb.executeQuery(query.toString(), Map.class, params.toArray());
    }

    public LocalDateTime selectDatetime() {
        return simpleDb.executeQuery(query.toString(), LocalDateTime.class, params.toArray());
    }

    public Long selectLong() {
        return simpleDb.executeQuery(query.toString(), Long.class, params.toArray());
    }

    public String selectString() {
        return simpleDb.executeQuery(query.toString(), String.class, params.toArray());
    }

    public Boolean selectBoolean() {
        return simpleDb.executeQuery(query.toString(), Boolean.class, params.toArray());
    }

    public List<Long> selectLongs() {
        List<Map<String, Object>> rows = simpleDb.executeQuery(query.toString(), List.class, params.toArray());
        return rows.stream()
                .map(row -> (Long) row.get("id")) // "id" 키에서 Long 값 추출
                .collect(Collectors.toList());
    }

    public <T> List<T> selectRows(Class<T> cls) {
        return simpleDb.selectRows(query.toString(), cls, params.toArray());
    }

    public <T> T selectRow(Class<T> cls) {
        return simpleDb.selectRow(query.toString(), cls, params.toArray());
    }
}
