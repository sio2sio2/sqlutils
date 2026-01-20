package edu.acceso.sqlutils.tx.functional;

import java.sql.Connection;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz funcional para lambdas que se usan en transacciones y devuelven un resultado.
 * @param <T> El tipo de dato del resultado.
 */
@FunctionalInterface
public interface TransactionableR<T> {
    /**
     * Ejecuta la operación en el contexto de una transacción.
     * @param conn La conexión a la base de datos.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     * @return El resultado de la operación.
     */
    T run(Connection conn) throws DataAccessException;
}