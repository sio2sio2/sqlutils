package edu.acceso.sqlutils.tx;

/**
 * Interface para escuchar eventos relacionados con transacciones.
 * Permite definir acciones a realizar en diferentes etapas
 * del ciclo de vida de una transacción, como commit, rollback y cierre.
 * Implementa el patrón de diseño Observer.
 */
public interface EventListener {

    /**
     * Evento que se dispara al iniciar una transacción.
     * al gestor de transacciones.
     * @param context El contexto del listener para la transacción actual.
     */
    default void onBegin(EventListenerContext context) {
    }

    /**
     * Evento que se dispara cuando se realiza un commit en una transacción.
     * @param context El contexto del listener para la transacción actual.
     */
    default void onCommit(EventListenerContext context) {
    }

    /**
     * Evento que se dispara cuando se realiza un rollback en una transacción.
     * @param context El contexto del listener para la transacción actual.
     */
    default void onRollback(EventListenerContext context) {
    }

    /**
     * Evento que se dispara cuando se cierra una transacción.
     * Por defecto, se ejecutan tanto onCommit como onRollback.
     * @param context El contexto del listener para la transacción actual.
     */
    default void onClose(EventListenerContext context) {
        onCommit(context);
        onRollback(context);
    }

    /**
     * Evento que se dispara al iniciar una transacción anidada que no es la raíz.
     * @param context El contexto del listener para la transacción actual.
     */
    default void onTransactionStart(EventListenerContext context) {
    }

    /**
     * Evento que se dispara al acabar una transacción anidada que no es la raíz.
     * @param context El contexto del listener para la transacción actual.
     */
    default void onTransactionEnd(EventListenerContext context) {
    }   
}
