package edu.acceso.sqlutils.transaction;

import java.sql.Connection;
import java.sql.SQLException;

import edu.acceso.sqlutils.errors.DataAccessException;

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

    public static void transactionSQL(Connection conn, Transactionable operations) throws DataAccessException {
        boolean originalAutoCommit = true;
        boolean rollback = false;

        try {
            if(conn == null) throw new IllegalArgumentException("La conexión no puede ser nula");
            if(!conn.isValid(0)) throw new SQLException("La conexión debe ser válida");
            conn.setAutoCommit(false);
            
            operations.run(conn);
        }
        catch(DataAccessException | RuntimeException | SQLException err) {
            try {
                rollback = true;
                conn.rollback();
                throw err;
            }
            catch(SQLException e) {
                throw new DataAccessException(e);
            }
        }
        finally {
            try {
                if(!originalAutoCommit && !rollback) conn.commit();
            }
            catch(SQLException err) {
                throw new DataAccessException(err);
            }
        }
    }
}

