package edu.acceso.sqlutils;

import java.sql.Connection;
import java.sql.SQLException;

public class Transaction {

    public static class Manager implements AutoCloseable {

        private Connection conn;
        private boolean committed;
        private boolean originalAutoCommit;

        public Manager(Connection conn) throws SQLException {
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

    @FunctionalInterface
    public static interface Transactionable {
        void run(Connection conn) throws DataAccessException;
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
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
        catch(DataAccessException err) {
            try {
                conn.rollback();
                rollback = true;
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