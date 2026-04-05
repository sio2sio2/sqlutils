package edu.acceso.sqlutils.tx;

import java.sql.Connection;

import edu.acceso.sqlutils.tx.event.EventListener;

/**
 * Interfaz que representa el contexto de una transacción.
 * Este objeto será el que se haga accesible al crear una transacción
 * mediante los métodos {@link edu.acceso.sqlutils.internal.tx.BaseTransactionManager#transaction(TransactionableR<T>)}
 * y {@link edu.acceso.sqlutils.internal.tx.BaseTransactionManager#transaction(Transactionable)}.
 * @param <R> El tipo del recurso que maneja la transacción (p. ej. {@link Connection} si usamos bases de datos con JDBC).
 */
public interface TransactionContext<R> {

    /**
     * Devuelve el nivel de anidamiento de la transacción actual.
     * @return El nivel de anidamiento.
     */
    public int level();

    /**
     * Devuelve la clave identificativa del gestor de transacciones.
     * @return La clave solicitada.
     */
    public String key();

    /**
     * Devuelve el recurso asociado a este contexto.
     * @return El recurso solicitado.
     */
    public R handle();

    /**
     * Devuelve el listener de eventos asociado a la clave dada.
     * @param key La clave que identifica al listener.
     * @return El listener solicitado, o {@code null} si no existe ningún listener asociado a esa clave.
     */
    public EventListener getEventListener(String key);

    /**
     * Devuelve el listener de eventos asociado a la clave dada.
     * @param <T> El tipo del listener a obtener.
     * @param key La clave que identifica al listener.
     * @param type La clase del listener solicitado.
     * @return El listener solicitado, o {@code null} si no existe ningún listener asociado a esa clave.
     */
    public <T extends EventListener> T getEventListener(String key, Class<T> type);
}