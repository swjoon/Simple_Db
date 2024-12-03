package simpleDb;

import com.fasterxml.jackson.databind.ObjectMapper;
import sql.Sql;
import sql.SqlImpl;

import java.sql.*;

public class SimpleDbImpl implements SimpleDb{
    private final String url;
    private final String username;
    private final String password;
    private ObjectMapper om = new ObjectMapper();

    public SimpleDbImpl(String host, int port, String dbName, String username, String password) {
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?serverTimezone=Asia/Seoul&useLegacyDatetimeCode=false&characterEncoding=UTF-8&useUnicode=true";
        this.username = username;
        this.password = password;
    }

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException("데이터베이스 연결 실패", e);
        }
    }

    public void run(String sql, Object... params) {
        try (Connection connection = createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("데이터베이스 입력 오류", e);
        }
    }

    public Sql genSql(){
        return new SqlImpl(createConnection());
    }

}
