package edu.acceso.sqlutils.dao;

import edu.acceso.sqlutils.errors.DataAccessException;

public interface DaoConnection {

    @FunctionalInterface
    public interface DaoTransactionable {
        void run(Dao dao) throws DataAccessException;
    }

    public Dao getDao();
    public void transaction(DaoTransactionable operaciones) throws DataAccessException;
}
