package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;

import edu.acceso.sqlutils.Entity;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.transaction.TransactionManager;

public interface DaoConnection {

    @FunctionalInterface
    public interface DaoTransactionable {
        void run(Dao dao) throws DataAccessException;
    }

    public Connection getConnection() throws DataAccessException;
    public Class<? extends Crud<? extends Entity>>[] getDaoClasses();
    public Dao getDao();

    default void transaction(DaoTransactionable operaciones) throws DataAccessException {
        try(Connection conn = getConnection()) {
            TransactionManager.transactionSQL(conn, c -> {
                operaciones.run(new Dao(c, getDaoClasses()));
            });
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }
}
