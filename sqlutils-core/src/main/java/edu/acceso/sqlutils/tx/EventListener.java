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
     */
    default void onBegin() {}

    /**
     * Evento que se dispara cuando se realiza un commit en una transacción.
     */
    default void onCommit() {}

    /**
     * Evento que se dispara cuando se realiza un rollback en una transacción.
     */
    default void onRollback() {}

    /**
     * Evento que se dispara al iniciar una transacción anidada que no es la raíz.
     */
    default void onTransactionStart() {}

    /**
     * Evento que se dispara al acabar una transacción anidada que no es la raíz.
     */
    default void onTransactionEnd() {}   
}