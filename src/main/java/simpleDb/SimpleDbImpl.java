package simpleDb;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import sql.Sql;
import sql.SqlImpl;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@RequiredArgsConstructor
public class SimpleDbImpl implements SimpleDb {
    private final int port;
    private final String host;
    private final String dbName;
    private final String username;
    private final String password;
    private ObjectMapper om = new ObjectMapper() {{
        registerModule(new JavaTimeModule());
        setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }};

    private final int maxPoolSize = 10;
    private final Queue<Connection> availableConnections = new ConcurrentLinkedQueue<>();
    private final Set<Connection> usedConnections = ConcurrentHashMap.newKeySet();

    private final ThreadLocal<Connection> transactionConnection = new ThreadLocal<>();
    private final ThreadLocal<Boolean> inTransaction = ThreadLocal.withInitial(() -> false);

    private Connection getConnection() {
        if (inTransaction.get() && transactionConnection.get() != null) {
            return transactionConnection.get();
        }

        Connection connection = availableConnections.poll();

        if (connection != null) {
            usedConnections.add(connection);
            return connection;
        }

        if (usedConnections.size() < maxPoolSize) {
            connection = createNewConnection();
            usedConnections.add(connection);
            return connection;
        }

        throw new RuntimeException("No available connections. Max pool size reached.");
    }

    private Connection createNewConnection() {
        String url = String.format("jdbc:mysql://%s:%d/%s?serverTimezone=Asia/Seoul", host, port, dbName);
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException("Database 연결 실패: " + e.getMessage(), e);
        }
    }

    private void releaseConnection(Connection connection) {
        if (usedConnections.remove(connection)) {
            availableConnections.add(connection);
        } else {
            throw new IllegalArgumentException("Connection does not belong to the pool");
        }
    }

    private void closeConnection(Connection connection) {
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Database 연결 닫기 실패: " + e.getMessage(), e);
        }
    }

    private <T> T _run(String sql, Class<T> type, Object... params) {
        Connection connection = getConnection();

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            bindParams(preparedStatement, params);
            if (sql.trim().toUpperCase().startsWith("INSERT")) {
                return (T) (Integer) preparedStatement.executeUpdate();
            }

            if (sql.trim().toUpperCase().startsWith("SELECT")) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    return parseResultSet(resultSet, type);
                }
            }

            return (T) (Integer) preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("데이터베이스 execute 실패: " + e.getMessage(), e);
        } finally {
            // 트랜잭션중이면 connection 반납 안함
            if (!inTransaction.get()) {
                releaseConnection(connection);
            }
        }
    }

    private void bindParams(PreparedStatement preparedStatement, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }
    }

    private <T> T parseResultSet(ResultSet resultSet, Class<T> type) throws SQLException {
        if (!resultSet.next()) throw new NoSuchElementException("No data found");

        return switch (type.getSimpleName()) {
            case "String" -> (T) resultSet.getString(1);
            case "List" -> {
                List<Map<String, Object>> rows = new ArrayList<>();
                do {
                    rows.add(resultSetToMap(resultSet));
                } while (resultSet.next());
                yield (T) rows;
            }
            case "Map" -> (T) resultSetToMap(resultSet);
            case "LocalDateTime" -> (T) resultSet.getTimestamp(1).toLocalDateTime();
            case "Long" -> (T) (Long) resultSet.getLong(1);
            case "Boolean" -> (T) (Boolean) resultSet.getBoolean(1);
            default -> throw new IllegalArgumentException("Unsupported class type: " + type.getSimpleName());
        };
    }

    private Map<String, Object> resultSetToMap(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        Map<String, Object> row = new LinkedHashMap<>();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = switch (metaData.getColumnType(i)) {
                case Types.BIGINT -> resultSet.getLong(columnName);
                case Types.TIMESTAMP -> {
                    Timestamp timestamp = resultSet.getTimestamp(columnName);
                    yield (timestamp != null) ? timestamp.toLocalDateTime() : null;
                }
                case Types.BOOLEAN -> resultSet.getBoolean(columnName);
                default -> resultSet.getObject(columnName);
            };
            row.put(columnName, value);
        }
        return row;
    }

    public <T> T run(String sql, Object... params) {
        return (T) _run(sql, Integer.class, params);
    }

    public <T> T executeQuery(String sql, Class<T> type, Object... params) {
        return _run(sql, type, params);
    }

    public <T> List<T> selectRows(String sql, Class<?> cls, Object... params) {
        return executeQuery(sql, List.class, params).stream().map(row -> om.convertValue(row, cls)).toList();

    }

    public <T> T selectRow(String sql, Class<?> cls, Object... params) {
        return (T) om.convertValue(executeQuery(sql, Map.class, params), cls);
    }

    public Sql genSql() {
        return new SqlImpl(this);
    }

    public void startTransaction(){
        if (inTransaction.get()) {
            throw new IllegalStateException("스레드에 이미 트랜잭션이 존재합니다");
        }
        Connection connection = getConnection();
        try {
            connection.setAutoCommit(false);
            usedConnections.add(connection);
            transactionConnection.set(connection);
            inTransaction.set(true);
        } catch (SQLException e) {
            throw new RuntimeException("트랜잭션 실행 실패: " + e.getMessage(), e);
        }
    }

    public void rollback(){
        if (!inTransaction.get()) {
            throw new IllegalStateException("스레드에 트랜잭션이 존재하지 않습니다");
        }
        try {
            Connection connection = transactionConnection.get();
            connection.rollback();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException("트랜잭션 롤백 실패: " + e.getMessage(), e);
        } finally {
            Connection connection = transactionConnection.get();
            releaseConnection(connection);
            transactionConnection.remove();
            inTransaction.remove();
        }
    }

    public void commit(){
        if (!inTransaction.get()) {
            throw new IllegalStateException("스레드에 트랜잭션이 존재하지 않습니다");
        }
        try {
            Connection connection = transactionConnection.get();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException("커밋 실패: " + e.getMessage(), e);
        } finally {
            Connection connection = transactionConnection.get();
            releaseConnection(connection);
            transactionConnection.remove();
            inTransaction.remove();
        }
    }
    // 사용 중인 커넥션 및 사용 가능한 커넥션 모두 닫기
    public void close() {
        availableConnections.forEach(this::closeConnection);
        usedConnections.forEach(this::closeConnection);
    }
}
