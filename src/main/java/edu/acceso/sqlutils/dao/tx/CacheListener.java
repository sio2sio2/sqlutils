package edu.acceso.sqlutils.dao.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.tx.EventListener;
import edu.acceso.sqlutils.tx.EventListenerContext;

/**
 * Listener de eventos que `permite agregar una caché de objetos a la transacción,
 * de forma que se puedan reutilizar objetos cargados en esa transacción.
 */
public class CacheListener implements EventListener {
    private static final Logger logger = LoggerFactory.getLogger(CacheListener.class);

    /**
     * Clave identificativa del gestor de caché.
     */
    public static final String KEY = new Object().toString();

    @Override
    public void onBegin(EventListenerContext context) {
        context.setResource(new Cache());
        logger.trace("Creada nueva caché de entidades para la transacción asociada a la conexión {}", context.key());
    }

    @Override
    public void onCommit(EventListenerContext context) {
        // No es necesario limpiar la caché, ya que todos los recursos se eliminan al finalizar la transacción.
    }

    @Override
    public void onRollback(EventListenerContext context) {
        // No es necesario limpiar la caché, ya que todos los recursos se eliminan al finalizar la transacción.
    }

    // Dejamos que se manipule directamente la caché.
}
