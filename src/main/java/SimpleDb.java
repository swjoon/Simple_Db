import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.util.stream.IntStream;

public class SimpleDb {
    private final String url;
    private final String username;
    private final String password;
    private ObjectMapper om = new ObjectMapper();

    public SimpleDb(String host, int port, String dbName, String username, String password) {
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?serverTimezone=Asia/Seoul&useLegacyDatetimeCode=false";
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
            throw new RuntimeException("데이터베이스 입력 실패", e);
        }
    }

}
