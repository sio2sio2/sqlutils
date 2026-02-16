package edu.acceso.sqlutils.tx;

/**
 * Interfaz que representa el subcontexto de transacción
 * que observa un listener de eventos.
 * Permite a los listeners durante una transacción almacenar
 * recursos asociados a su ejecución.
 */
public interface EventListenerContext {
    /**
     *  Nivel de anidamiento de la transacción.
     *  @return El nivel de anidamiento solicitado.
     */
    public int level();

    /**
     *  Clave identificativa del gestor de transacciones
     *  @return La clave identificativa del gestor de transacciones.
     */
    public String key();

    /**
     * Establece un recurso asociado al listener en el contexto de la transacción.
     * @param <T> El tipo del recurso a asociar.
     * @param resource El recurso a asociar con el listener.
     */
    public <T> void setResource(T resource);

    /**
     * Obtiene el recurso asociado al listener en el contexto de la transacción
     * @param <T> El tipo del recurso a obtener.
     * @return El recurso asociado al listener, o {@code null} si no se ha establecido ningún recurso.
     */
    public <T> T getResource();
}
