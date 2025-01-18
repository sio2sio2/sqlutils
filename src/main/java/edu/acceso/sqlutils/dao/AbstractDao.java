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

    protected final ConnectionProvider cp;


    protected AbstractDao(DataSource ds) {
        cp = ConnectionProvider.fromDataSource(ds);
    }

    protected AbstractDao(Connection conn) {
        cp = ConnectionProvider.fromConnection(conn);
    }
}
