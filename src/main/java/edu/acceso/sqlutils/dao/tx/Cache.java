package edu.acceso.sqlutils.dao.tx;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.relations.RelationEntity;

/**
 * Caché de entidades cargadas durante una transacción DAO.
 * Permite reutilizar instancias ya cargadas para optimizar el acceso a datos relacionados.
 * TODO:: ¿Y si la entidad enlaza (Fk) con otra u otras entidades? ¿No habría que guardarlas también?
 */
public class Cache {
    private static final Logger logger = LoggerFactory.getLogger(Cache.class);

    /** Mapa que almacena las entidades en caché, indexadas por su hashcode */
    private final Map<Integer, Entity> cache;

    /** Constructor que inicializa la caché */
    public Cache() {
        cache = new HashMap<>();
    }

    /** Obtiene una entidad de la caché por su hashcode
     * @param key Hashcode de la entidad
     * @return Entidad almacenada en la caché o {@code null} si no existe
     */
    public Entity get(int key) {
        Entity entity = cache.get(key);
        if (entity != null) logger.trace("Entidad recuperada de la caché: {}", entity);
        else logger.trace("No se encontró entidad en la caché para el hash {}.", key);
        return entity;
    }

    /** Obtiene una entidad de la caché usando su clase e ID
     * @param <E> Tipo de entidad
     * @param entityClass Clase de la entidad
     * @param id ID de la entidad
     * @return Entidad almacenada en la caché o {@code null} si no existe
     */
    @SuppressWarnings("unchecked")
    public <E extends Entity> E get(Class<E> entityClass, Long id) {
        int key = RelationEntity.hash(entityClass, id);
        return (E) get(key);
    }

    /** Comprueba si una entidad está almacenada en la caché
     * @param entity Entidad a comprobar
     * @return {@code true} si la entidad está en la caché, {@code false} en caso contrario
     */
    public boolean contains(Entity entity) {
        int key = RelationEntity.hash(entity);
        return cache.containsKey(key);
    }

    /** Comprueba si una entidad está almacenada en la caché
     * @param entityClass Clase de la entidad
     * @param id ID de la entidad
     * @return {@code true} si la entidad está en la caché, {@code false} en caso contrario
     */
    public boolean contains(Class<? extends Entity> entityClass, Long id) {
        int key = RelationEntity.hash(entityClass, id);
        return cache.containsKey(key);
    }
    
    /** Almacena una entidad en la caché
     * @param <E> Tipo de entidad
     * @param entity Entidad a almacenar. Si es {@code null}, no se almacena nada.
     * @return Entidad previamente almacenada con la misma clave o {@code null} si no existía.
     */
    public <E extends Entity> E put(E entity) {
        if(entity == null) return null;

        int key = RelationEntity.hash(entity);
        @SuppressWarnings("unchecked")
        E entOld = (E) cache.put(key, entity);

        if (entOld == null) logger.trace("Se almacena en caché la entidad con hash {}.", key);
        else logger.trace("La entidad con hash {} ya estaba almacenada en la caché.", key);

        return entOld;
    }

    /** Elimina una entidad de la caché
     * @param <E> Tipo de entidad.
     * @param entityClass Clase de la entidad. Si es {@code null}, no se elimina nada.
     * @param id ID de la entidad. Si es {@code null}, no se elimina nada.
     * @return Entidad eliminada o {@code null} si no existía.
     */
    public <E extends Entity> E delete(Class<? extends Entity> entityClass, Long id) {
        if(entityClass == null || id == null) return null;
        int key = RelationEntity.hash(entityClass, id);

        @SuppressWarnings("unchecked")
        E result = (E) cache.remove(key);
        if(result != null) logger.trace("Se elimina de la caché la entidad con hash {}.", key);
        return result;
    }

    /** Elimina una entidad de la caché
     * @param <E> Tipo de entidad.
     * @param entity Entidad a eliminar. Si es {@code null}, no se elimina nada.
     * @return Entidad eliminada o {@code null} si no existía.
     */
    public <E extends Entity> E delete(E entity) {
        if(entity == null) return null;
        return delete(entity.getClass(), entity.getId());
    }   
}
