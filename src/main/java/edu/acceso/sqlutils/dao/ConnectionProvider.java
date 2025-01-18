package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Proporciona una conexión de un DataSource o una propia conexión.
 */
public abstract class ConnectionProvider {
    public abstract Connection getConnection() throws SQLException;

    public static ConnectionProvider fromDataSource(DataSource ds) {
        return new ConnectionProvider() {
            @Override
            public Connection getConnection() throws SQLException {
                return ds.getConnection();
            }
        };
    }

    public static ConnectionProvider fromConnection(Connection conn) {
        return new ConnectionProvider() {
            @Override
            public Connection getConnection() throws SQLException {
                // Gracias a esto conn no se cerrará aunque se intente
                // cerrar (con un try-with-resources) en la implementación.
                return ConnectionProxy.wrap(conn, false);
            }
        };
    }
}