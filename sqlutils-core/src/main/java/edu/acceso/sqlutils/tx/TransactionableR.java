package edu.acceso.sqlutils.tx;

/**
 * Interfaz funcional para operaciones de transacción que devuelven un valor.
 * @param <S> Tipo del recurso de la transacción (Connection, EntityManager, etc.).
 * @param <T> Tipo del valor devuelto.
 */
@FunctionalInterface
public interface TransactionableR<S, T> {
    /**
     * Ejecuta las operaciones de la transacción.
     * @param ctxt El contexto de la transacción.
     * @return El valor devuelto por las operaciones.
     * @throws Throwable Si ocurre un error durante las operaciones.
     */
    T run(TransactionContext<S> ctxt) throws Throwable;
}