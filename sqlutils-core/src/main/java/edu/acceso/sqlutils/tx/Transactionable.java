package edu.acceso.sqlutils.tx;

/**
 * Interfaz funcional para operaciones de transacción sin valor devuelto.
 * @param <S> Tipo del recurso de la transacción (Connection, EntityManager, etc.).
 */
@FunctionalInterface
public interface Transactionable<S> {
    /**
     * Ejecuta las operaciones de la transacción.
     * @param ctxt El contexto de la transacción.
     * @throws Throwable Si ocurre un error durante las operaciones.
     */
    void run(TransactionContext<S> ctxt) throws Throwable;
}