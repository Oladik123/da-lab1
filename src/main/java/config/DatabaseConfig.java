package config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DatabaseConfig {
    private static final Logger logger = LogManager.getLogger(DatabaseConfig.class.getName());
    static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/osm";
    static final String USER = "postgres";
    static final String PASS = "postgres";

    public static void initTables() {
        logger.info("started script init tables");
        try (Connection con = DriverManager.getConnection(DB_URL, USER, PASS)) {
            PreparedStatement pst = con.prepareStatement("CREATE TABLE IF NOT EXISTS nodes (\n" +
                    "                                       id serial PRIMARY KEY,\n" +
                    "                                       node_id INTEGER ,\n" +
                    "                                       version INTEGER,\n" +
                    "                                       timestamp date,\n" +
                    "                                       uid INTEGER,\n" +
                    "                                       \"user\" VARCHAR(100),\n" +
                    "                                       changeset INTEGER,\n" +
                    "                                       lat double precision,\n" +
                    "                                       lon double precision\n" +
                    "\n" +
                    "\n" +
                    ")");
            pst.execute();

            pst = con.prepareStatement("CREATE TABLE IF NOT EXISTS tags (\n" +
                    "                                     id serial PRIMARY KEY,\n" +
                    "                                     key VARCHAR(100),\n" +
                    "                                     value VARCHAR(100),\n" +
                    "                                     constraint node_id foreign key (id) references nodes (id)\n" +
                    ");");
            pst.execute();

            logger.info("tables created successfully");

        } catch (SQLException ex) {
            logger.error("error on creating tables");
            ex.printStackTrace();
        }
    }

    public static void dropTables() {
        logger.info("started script drop tables");
        try (Connection con = DriverManager.getConnection(DB_URL, USER, PASS)) {
            PreparedStatement pst = con.prepareStatement("DROP TABLE IF EXISTS tags");
            pst.execute();
            pst = con.prepareStatement("DROP TABLE IF EXISTS nodes");
            pst.execute();


            logger.info("tables deleted successfully");

        } catch (SQLException ex) {
            logger.error("error on deleting tables");
            ex.printStackTrace();
        }
    }


}
