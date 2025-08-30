package edu.acceso.sqlutils.dao.tx;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz funcional que define un consumidor de transacciones.
 * Permite ejecutar operaciones dentro de una misma transacción utilizando un objeto {@link TransactionContext}.
 * 
 * <p>
 * La razón de definarla y no usar {@link java.util.function.Consumer} directamente es que permite lanzar excepciones específicas de acceso a datos
 * sin necesidad de capturarlas, lo que facilita el manejo de errores en las operaciones de la base de datos.
 * </p>
 */
@FunctionalInterface
public interface TransactionalOperation {
    /**
     * Acepta un contexto de transacción y realiza una operación dentro de la misma.
     * @param tx El contexto de transacción.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    void accept(TransactionContext tx) throws DataAccessException;
}
