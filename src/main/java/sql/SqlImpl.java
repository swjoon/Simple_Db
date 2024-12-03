package sql;

import simpleDb.SimpleDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SqlImpl implements Sql {
    private final Connection conn;
    private final StringBuilder query;
    private final List<Object> params;

    public SqlImpl(Connection connection) {
        this.conn = connection;
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

    public int insert() {
        try {
            conn.setAutoCommit(false);

            try (PreparedStatement preparedStatement = conn.prepareStatement(query.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                }
                int id = preparedStatement.executeUpdate();
                conn.commit();
                return id;
            } catch (SQLException e) {
                conn.rollback();
                throw new RuntimeException("데이터베이스 INSERT Query 실패", e);
            }finally {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("데이터베이스 Connection 문제", e);
        }
    }

    public int update() {
        try {
            conn.setAutoCommit(false);

            try (PreparedStatement preparedStatement = conn.prepareStatement(query.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                }
                int affectedRowsCount = preparedStatement.executeUpdate();
                conn.commit();
                return affectedRowsCount;
            } catch (SQLException e) {
                conn.rollback();
                throw new RuntimeException("데이터베이스 INSERT Query 실패", e);
            }finally {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("데이터베이스 Connection 문제", e);
        }
    }

    public int delete() {
        try {
            conn.setAutoCommit(false);

            try (PreparedStatement preparedStatement = conn.prepareStatement(query.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                }
                int affectedRowsCount = preparedStatement.executeUpdate();
                conn.commit();
                return affectedRowsCount;
            } catch (SQLException e) {
                conn.rollback();
                throw new RuntimeException("데이터베이스 INSERT Query 실패", e);
            }finally {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("데이터베이스 Connection 문제", e);
        }
    }

}
