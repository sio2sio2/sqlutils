package edu.acceso.sqlutils.transaction;

import java.sql.Connection;

import edu.acceso.sqlutils.errors.DataAccessException;

@FunctionalInterface
public interface Transactionable {
    void run(Connection conn) throws DataAccessException;
}