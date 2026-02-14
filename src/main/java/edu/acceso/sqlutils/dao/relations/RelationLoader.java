package edu.acceso.sqlutils.dao.relations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Cargador de relaciones.
 * Proporciona métodos comunes para cargar entidades relacionadas.
 * 
 * @param <E> Tipo de entidad que este cargador se encargará de cargar.
 * 
 * <p>Contiene una referencia al cargador de relaciones previo que originó el DAO que creó este cargador.
 * De este modo, se puede mantener un historial de entidades cargadas para detectar ciclos infinitos.</p>
 */
public abstract class RelationLoader<E extends Entity> {
    private static final Logger logger = LoggerFactory.getLogger(RelationLoader.class);

    /** DAO que permite cargar la entidad que maneja este cargador de relaciones */
    protected final AbstractCrud<E> dao;
    /** Cargador de relaciones que cargó la entidad que usa este cargador */
    protected final RelationLoader<? extends Entity> previous;

    /** Entidad que carga este cargador */
    protected RelationEntity<E> rl;

    /** Clase de la entidad que carga este cargador de relaciones */
    protected final Class<E> entityClass;

    /**
     * Construye un nuevo cargador de relaciones a partir de un DAO.
     * 
     * @param originalDao DAO a partir del cual se crea el cargador de relaciones
     * @param entityClass Clase de la entidad que carga este cargador de relaciones
     * @throws DataAccessException Si no pueede crear el cargador de relaciones a partir del DAO proporcionado
     */
    public RelationLoader(AbstractCrud<? extends Entity> originalDao, Class<E> entityClass) throws DataAccessException {
        this.entityClass = entityClass;
        previous = null;
        dao = originalDao.createLinkedDao(this);
        logger.debug("Creado cargador de relaciones para una entidad '{}' a partir de un DAO para una entidad '{}'", 
            entityClass.getSimpleName(), originalDao.getEntityClass().getSimpleName());
    }

    /**
     * Construye un nuevo cargador de relaciones a partir de otro previo.
     * @param previous Cargador de relaciones previo
     * @param entityClass Clase de la entidad que carga este cargador de relaciones
     * @throws DataAccessException Si no pueede crear el cargador de relaciones a partir del cargador previo proporcionado
     */
    public RelationLoader(RelationLoader<? extends Entity> previous, Class<E> entityClass) throws DataAccessException {
        this.entityClass = entityClass;
        this.previous = previous;
        this.dao = previous.getDao().createLinkedDao(this);
        logger.debug("Creado cargador de relaciones para una entidad '{}' a partir de otro para una entidad '{}'", 
            entityClass.getSimpleName(), previous.getEntityClass().getSimpleName());
    }

    /**
     * Carga la entidad correspondiente a una clave foránea.
     * 
     * @param id Clave foránea que identifica la entidad o {@code null} si no hay relación.
     * @return Entidad cargada o {@code null} si no hay relación
     * @throws DataAccessException Si ocurre un error al acceder a los datos
     */
    public E loadEntity(Long id) throws DataAccessException {
        // Si no hay relación, no es necesario cargar nada.
        if(id == null) return null;

        // Establecemos el RelationEntity asoaciado a este cargador
        setRl(id);

        // Comprobamos si la entidad ya está en caché
        E cachedEntity = dao.getCache().get(getEntityClass(), id);
        if(cachedEntity != null) return cachedEntity;

        // Como la entidad no se cargó anteriormente, la cargamos
        // usando la estrategia concreta del cargador.
        return loadEntityNotPreviouslyLoaded(id);
    }

    /**
     * Termina de cargar la entidad relacionada, si no se encontraba ya en caché.
     * @param id ID de la entidad relacionada.
     * @return Entidad cargada.
     * @throws DataAccessException Si ocurre un error al acceder a los datos
     */
    protected abstract E loadEntityNotPreviouslyLoaded(Long id) throws DataAccessException;

    /**
     * Comprueba si el cargador ya ha cargado la entidad asociada y, en caso de que no lo haya hecho.
     * detecta si la entidad ya se había cargado por algún cargador previo.
     * 
     * @param relationEntity Entidad a comprobar
     * @return {@code true} si es la primera de un ciclo, {@code false} en caso contrario
     */
    @SuppressWarnings("unchecked")
    protected boolean isInHistory() {
        RelationEntity<E> relationEntity = this.getRl();
        RelationLoader<? extends Entity> previous = this.previous;

        if(isAlreadyLoaded()) return true;

        while(previous != null) {
            if(relationEntity.equals(previous.getRl())) {
                relationEntity.setLoadedEntity((E) previous.getRl().getLoadedEntity());
                logger.debug("Detectado ciclo de referencias para la entidad '{}' con ID {}", 
                    relationEntity.getEntityClass().getSimpleName(), relationEntity.getId());
                return true;
            }
            previous = previous.previous;
        }

        return false;
    }

    /**
     * Informa de si el cargador ya cargó la entidad asociada.
     * @return {@code true} si la entidad ya ha sido cargada, {@code false} en caso contrario
     */
    protected boolean isAlreadyLoaded() {
        RelationEntity<E> relationEntity = this.getRl();
        return relationEntity.getLoadedEntity() != null;
    }

    /**
     * Establece la entidad asociada a este cargador de relaciones.
     * 
     * @param id ID de la entidad
     */
    protected void setRl(Long id) {
        this.rl = new RelationEntity<>(dao.getEntityClass(), id);
    }

    /**
     * Obtiene la entidad asociada a este cargador de relaciones.
     * 
     * @return La entidad solicitada.
     */
    protected RelationEntity<E> getRl() {
        if(rl == null) throw new IllegalStateException("No se ha establecido la entidad asociada a este cargador de relaciones.");
        return rl;
    }

    /**
     * Obtiene el DAO original a partir del cual se creó este cargador de relaciones.
     * 
     * @return DAO original
     */
    public AbstractCrud<? extends Entity> getDao() {
        return dao;
    }

    /**
     * Obtiene la clase de la entidad que carga este cargador de relaciones.
     * 
     * @return Clase de la entidad
     */
    public Class<E> getEntityClass() {
        return entityClass;
    }
}