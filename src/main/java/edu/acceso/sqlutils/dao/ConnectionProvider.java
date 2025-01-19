package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Abstrae la creación y elimación de objetos Connection a partir de un DataSource
 * (pool de conexiones) y directamente de un objeto Connection.
 */
public abstract class ConnectionProvider {
    protected boolean closeable;

    public abstract Connection getConnection() throws SQLException;

    public boolean isCloseable() {
        return closeable;
    }

    /**
     * Genera un ConnectionProvider cuyo método getConnection() crea una
     * conexión a partir de un DataSource.
     * @param ds El DataSource proporcionado por un pool de conexión.
     * @return El objeto ConnectionProvider.
     */
    public static ConnectionProvider fromDataSource(DataSource ds) {
        return new ConnectionProvider() {
            @Override
            public Connection getConnection() throws SQLException {
                closeable = true;
                return ds.getConnection();
            }
        };
    }

    /**
     * Genera un ConnectionProvider cuyo método .getConnection() devuelve
     * un envoltorio del propio objeto Connection que se le proporciona como
     * parámetro. El envoltorio actúa exactamente igual que el objeto, con
     * la excepción de que su método .close() no cierra el objeto de conexión.
     * @param conn El objeto Connection.
     * @return El objeto ConnectionProvider.
     */
    public static ConnectionProvider fromConnection(Connection conn) {
        return new ConnectionProvider() {
            @Override
            public Connection getConnection() throws SQLException {
                closeable = false;
                // Gracias a esto conn no se cerrará aunque se intente
                // cerrar (con un try-with-resources) en la implementación.
                return ConnectionProxy.wrap(conn, closeable);
            }
        };
    }
}