package verification;

import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String INFINITE_TIMESTAMP = "2038-12-12 00:00:01";
    private static String DB_URL = "jdbc:mysql://localhost:3306/data";
    private static String DB_USER = "root";
    private static String DB_PASSWORD = "root";

    public static java.sql.Connection getDBConnection() {

        // Load the Connector/J driver
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
