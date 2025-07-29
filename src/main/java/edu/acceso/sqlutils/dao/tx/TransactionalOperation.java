package edu.acceso.sqlutils.dao.tx;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz funcional que define un consumidor de transacciones.
 * Permite ejecutar operaciones dentro de una misma transacción utilizando un objeto {@link TransactionContext}.
 * 
 * <p>
 * La razón de definarla y no usar {@link Consumer} directamente es que permite lanzar excepciones específicas de acceso a datos
 * sin necesidad de capturarlas, lo que facilita el manejo de errores en las operaciones de la base de datos.
 * </p>
 */
@FunctionalInterface
public interface TransactionalOperation {
    void accept(TransactionContext tx) throws DataAccessException;
}
