package edu.acceso.sqlutils.tx;

import java.sql.Connection;

/**
 * Interfaz que representa el contexto de una transacción.
 * Este objeto será el que se haga accesible al crear una transacción
 * mediante los métodos {@link TransactionManager#transaction(TransactionableR<T>)}
 * y {@link TransactionManager#transaction(Transactionable)}.
 */
public interface TransactionContext {

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
     * Devuelve la conexión protegida asociada a este contexto.
     * @return La conexión solicitada.
     */
    public Connection connection();

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

    /**
     * Devuelve el recurso asociado al listeners identificado por la clave proporcionada.
     * @param key La clave que identifica al listener.
     * @return El recurso solicitado, o {@code null} si no existe ningún recurso asociado a esa clave.
     */
    public Object getResource(String key);

    /**
     * Devuelve el recurso asociado al listener identificado por la clave proporcionada.
     * @param <T> El tipo del recurso a obtener.
     * @param key La clave que identifica al listener.
     * @param type La clase del recurso solicitado.
     * @return El recurso solicitado, o {@code null} si no existe ningún recurso asociado a esa clave.
     */
    public <T> T getResource(String key, Class<T> type);
}