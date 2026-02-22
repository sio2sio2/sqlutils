package edu.acceso.sqlutils.dao.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.tx.ContextAwareEventListener;

/**
 * Listener de eventos que `permite agregar una caché de objetos a la transacción,
 * de forma que se puedan reutilizar objetos cargados en esa transacción.
 */
public class CacheListener extends ContextAwareEventListener {
    private static final Logger logger = LoggerFactory.getLogger(CacheListener.class);

    /**
     * Clave identificativa del gestor de caché.
     */
    public static final String KEY = new Object().toString();

    @Override
    public Object createResource() {
        String key = getContext().key();
        logger.trace("Creada nueva caché de entidades para la transacción asociada a la conexión {}.", key);
        return new Cache();
    }

    /*
    @Override
    public void onBegin() {
        EventListenerContext context = getContext();

        context.setResource(new Cache());
    }
    */

    // No es necesario limpiar la caché, ya que todos los recursos se eliminan al finalizar la transacción.

    /**
     * Se obtiene la caché para que pueda manipularse durante la transacción.
     * @return La caché asociada a la transacción actual.
     */
    public Cache getCache() {
        return (Cache) getContext().getResource();
    }
}
