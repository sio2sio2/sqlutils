package edu.acceso.sqlutils.tx;

import java.util.function.Supplier;

/**
 * Extiende la interfaz EventListener para permitir que el listener
 * tenga acceso al contexto de la transacción en cualquier momento.
 */
public abstract class ContextAwareEventListener implements EventListener {
    
    private Supplier<EventListenerContext> contextSupplier;

    /**
     * Permite establecer el mecanismo que proporciona al listener su contexto de la transacción actual.
     * Al registrar el listener con {@link TransactionManager#addListener(String, EventListener)},
     * el gestor genera el proveedor de contexto e invoca este método para facilitárselo al listener.
     * @param contextSupplier El proveedor del contexto proporcionado por el gestor de transacciones.
     */
    protected void setContextSupplier(Supplier<EventListenerContext> contextSupplier) {
        this.contextSupplier = contextSupplier;
    }

    /**
     * Construye un recurso específico para el listener.
     * @return El recurso específico que desea manejar el listener, o {@code null} si no se requiere ninguno.
     */
    public Object createResource() {
        return null;
    }

    /**
     * Obtiene el contexto de la transacción actual utilizando el proveedor previamente
     * suministrado. Este método puede ser invocado por cualquier método del listener
     * para acceder a información sobre la transacción, incluido el recurso específico del listener.
     * Véase {@link EventListenerContext} para más detalles sobre la información disponible en el contexto.
     * @return El contexto de la transacción actual.
     */
    protected EventListenerContext getContext() {
        return contextSupplier.get();
    }
}
