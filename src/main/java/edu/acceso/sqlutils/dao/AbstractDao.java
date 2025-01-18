package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Clase abstracta de la que deben derivar las clases DAO concretas
 * a fin de que utilicen un DataSource o directamente una Conexión
 * para hacer las operaciones con la base de datos.
 */
public abstract class AbstractDao {

    private final DataSource ds;
    private final Connection conn;


    protected AbstractDao(DataSource ds) {
        this.ds = ds;
        this.conn = null;
    }

    protected AbstractDao(Connection conn) {
        this.ds = null;
        // Gracias a esto conn no se cerrará aunque se intente
        // cerrar (con un try-with-resources) en la implementación.
        this.conn = ConnectionProxy.createProxy(conn, false);
    }

    protected Connection getConnection() throws SQLException {
        return ds == null?conn:ds.getConnection();
    }
}
