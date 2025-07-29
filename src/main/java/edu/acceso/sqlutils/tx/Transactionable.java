package edu.acceso.sqlutils.tx;

import java.sql.Connection;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 *  Interfaz funcional para lambdas que se usan en transacciones.
 */
@FunctionalInterface
public interface Transactionable {
    void run(Connection conn) throws DataAccessException;
}