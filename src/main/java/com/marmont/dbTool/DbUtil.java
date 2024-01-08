package com.marmont.dbtool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DbUtil {

    private static final Logger LOGGER = Logger.getLogger(DbUtil.class.getName());

    private String dbUrl = "jdbc:mysql://localhost:3306/training";
    private String username = "training";
    private String password = "training";

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl, username, password);
    }

    public static void close(PreparedStatement preparedStatement, Connection connection) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Fehler beim Schließen des PreparedStatement", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Fehler beim Schließen der Connection", e);
            }
        }
    }
}
