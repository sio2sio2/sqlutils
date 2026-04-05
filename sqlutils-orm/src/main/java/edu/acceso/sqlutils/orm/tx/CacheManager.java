package edu.acceso.sqlutils.orm.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.orm.minimal.Entity;
import edu.acceso.sqlutils.tx.event.ContextAwareEventListener;

/**
 * Listener de eventos que `permite agregar una caché de objetos a la transacción,
 * de forma que se puedan reutilizar objetos cargados en esa transacción.
 */
public class CacheManager extends ContextAwareEventListener {
    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);

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

    // No es necesario limpiar la caché, ya que todos los recursos se eliminan al finalizar la transacción.

    /**
     * Se obtiene la caché para que pueda manipularse durante la transacción.
     * @return La caché asociada a la transacción actual.
     */
    private Cache getCache() {
        return (Cache) getContext().getResource();
    }

    /**
     * Agrega una entidad a la caché de la transacción actual.
     * @param <E> Tipo de entidad a agregar.
     * @param entity La entidad a agregar a la caché.
     * @return La entidad agregada a la caché, o la instancia existente si ya estaba presente.
     */
    public <E extends Entity> E putInCache(E entity) {
        getCache().put(entity);
        return entity;
    }

    /**
     * Elimina una entidad de la caché de la transacción actual utilizando su clase e ID.
     * @param <E> Tipo de entidad a eliminar.
     * @param clazz Clase de la entidad a eliminar.
     * @param id ID de la entidad a eliminar.
     * @return La entidad eliminada de la caché, o {@code null} si no se encontró.
     */
    public <E extends Entity> E deleteFromCache(Class<E> clazz, Long id) {
        return getCache().delete(clazz, id);
    }

    /**
     * Verifica si una entidad con la clase e ID especificados está presente en la caché de la transacción actual.
     * @param clazz  Clase de la entidad a verificar.
     * @param id ID de la entidad a verificar.
     * @return {@code true} si la entidad está presente en la caché, {@code false} en caso contrario.
     */
    public boolean isInCache(Class<? extends Entity> clazz, Long id) {
        return getCache().contains(clazz, id);
    }

    /**
     * Obtiene una entidad de la caché de la transacción actual utilizando su clase e ID.
     * @param <E> Tipo de entidad a obtener.
     * @param clazz Clase de la entidad a obtener.
     * @param id ID de la entidad a obtener.
     * @return La entidad obtenida de la caché o {@code null} si no existe.
     */
    public <E extends Entity> E get(Class<E> clazz, Long id) {
        return getCache().get(clazz, id);
    }   
}
