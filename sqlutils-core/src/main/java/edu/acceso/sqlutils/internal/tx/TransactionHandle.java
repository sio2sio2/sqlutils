package edu.acceso.sqlutils.internal.tx;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz que permite soportar transacciones ocultando cuál es el recurso subyacente que se utiliza.
 *
 * @param <R> El tipo de recurso subyacente.
 */
public interface TransactionHandle<R> {

    /**
     * Implementa cómo iniciar la transacción usando el recurso subyacente.
     * @throws DataAccessException Si ocurre algún error al iniciar la transacción con el recurso subyacente.
     */
    public void begin() throws DataAccessException;
    /**
     * Implementa cómo hacer commit de la transacción usando el recurso subyacente.
     * @throws DataAccessException Si ocurre algún error al hacer commit de la transacción con el recurso subyacente.
     */
    public void commit() throws DataAccessException;
    /**
     * Implementa cómo hacer rollback de la transacción usando el recurso subyacente.
     * @throws DataAccessException Si ocurre algún error al hacer rollback de la transacción con el recurso subyacente.
     */
    public void rollback() throws DataAccessException;
    /**
     * Implementa cómo cerrar el recurso subyacente.
     * @throws DataAccessException Si ocurre algún error al cerrar el recurso subyacente.
     */
    public void close() throws DataAccessException;
    /**
     * Verifica si el recurso subyacente que maneja esta transacción está abierto.
     * @return true si el recurso está abierto, false en caso contrario.
     */
    boolean isOpen();
    /**
     * Devuelve el recurso subyacente que maneja esta transacción.
     * @return El recurso solicitado.
     */
    R getHandle();
}