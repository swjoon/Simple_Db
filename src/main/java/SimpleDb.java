import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;

public class SimpleDb {
    private final String url;
    private final String username;
    private final String password;
    private ObjectMapper om = new ObjectMapper();

    public SimpleDb(String host, int port, String dbName, String username, String password) {
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
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

    public void run(String sql) {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement();
        ) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException("데이터베이스 입력 실패", e);
        }
    }

}
