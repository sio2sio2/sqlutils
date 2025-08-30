package edu.acceso.sqlutils.tx;

import java.sql.Connection;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 *  Interfaz funcional para lambdas que se usan en transacciones.
 */
@FunctionalInterface
public interface Transactionable {
    /**
     * Ejecuta la operación en el contexto de una transacción.
     * @param conn La conexión a la base de datos.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    void run(Connection conn) throws DataAccessException;
}