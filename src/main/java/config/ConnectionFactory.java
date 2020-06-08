package config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
    private static final Logger logger = LogManager.getLogger(ConnectionFactory.class.getName());
    static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/osm";
    static final String USER = "postgres";
    static final String PASS = "postgres";

    public static Connection getConnection() {
        try {
            return DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException ex) {
            throw new RuntimeException("Error connecting to the database", ex);
        }
    }

}
