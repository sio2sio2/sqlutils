package edu.acceso.sqlutils;

import java.sql.Connection;
import java.sql.SQLException;

public class TransactionManager implements AutoCloseable {

    private Connection conn;
    private boolean committed;
    private boolean originalAutoCommit;

    public TransactionManager(Connection conn) throws SQLException {
        setConn(conn);
        originalAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
    }

    public Connection getConn() {
        return conn;
    }

    private void setConn(Connection conn) throws SQLException {
        if(conn == null) throw new IllegalArgumentException("La conexión no puede ser nula");
        if(!conn.isValid(0)) throw new SQLException("La conexión debe ser válida");
        this.conn = conn;
    }

    public void commit() throws SQLException {
        conn.commit();
        committed = true;
    }

    public boolean isNested() {
        return !originalAutoCommit;
    }

    @Override
    public void close() throws SQLException {
        if(!committed && !isNested()) conn.rollback();
        conn.setAutoCommit(originalAutoCommit);
    }
}
