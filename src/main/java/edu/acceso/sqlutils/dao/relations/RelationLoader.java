package edu.acceso.sqlutils.dao.relations;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.DaoCrud;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Cargador de relaciones.
 * Proporciona métodos comunes para cargar entidades relacionadas y gestionar ciclos de referencia.
 * 
 * <p>
 * Básicamente implementa el historial de entidades cargadas para evitar ciclos y define un método 
 * {@link #getDao(Class)} para obtener un nuevo DAO que comparte el mismo cargador de relaciones.
 */
public abstract class RelationLoader {

    /** DAO a partir de cual se crea el cargador. */
    protected final DaoCrud<? extends Entity> originalDao;
    /** Historial de entidades cargadas para evitar duplicados. */
    protected final List<RelationEntity<? extends Entity>> historial = new ArrayList<>();
    /** Primera RelationEntity del ciclo, si existe. */
    protected RelationEntity<? extends Entity> loopBeginning = null;

    /**
     * Constructor.
     * 
     * @param dao DAO a partir del cual se crea el cargador de relaciones
     */
    public RelationLoader(DaoCrud<? extends Entity> dao) {
        this.originalDao = dao;
    }

    /**
     * Carga la entidad correspondiente a una clave foránea.
     * 
     * @param <T> Clase de la entidad
     * @param entityClass Clase de la entidad
     * @param id Clave foránea que identifica la entidad o {@code null} si no hay relación.
     * @return Entidad cargada o {@code null} si no hay relación
     * @throws DataAccessException Si ocurre un error al acceder a los datos
     */
    public abstract <T extends Entity> T loadEntity(Class<T> entityClass, Long id) throws DataAccessException;

    /**
     * Obtiene un DAO para la clase especificada a partir de otro DAO. La particularidad es que ambos
     * comparten el mismo cargador de relaciones.
     * @param <E> Clase de la entidad
     * @param entityClass Clase de la entidad
     * @return DAO para la entidad especificada
     * @throws DataAccessException Si ocurre un error al generar el DAO
     */
    protected <E extends Entity> DaoCrud<E> getDao(Class<E> entityClass) throws DataAccessException {
        return new DaoCrud<>(originalDao, entityClass);
    }

    /**
     * Comprueba si la entidad es la primera de un ciclo. Además, apunta en loopBeginning
     * cuál es esa primera entidad del ciclo en el historial.
     * 
     * @param relationEntity Entidad a comprobar
     * @return {@code true} si es la primera de un ciclo, {@code false} en caso contrario
     */
    protected boolean isLoaded(RelationEntity<? extends Entity> relationEntity) {
        if(historial.isEmpty() || relationEntity == null) return false;

        if(loopBeginning == null) {
            // ¿Está relationEntity ya en el historial? Lo apuntamos en ese caso.
            int idx = historial.indexOf(relationEntity);
            if(idx >= 0) loopBeginning = historial.get(idx);
        }

        return relationEntity.equals(loopBeginning);
    }

    /**
     * Obtiene la primera entidad del ciclo, si existe.
     * 
     * @return Primera entidad del ciclo o {@code null} si no hay ciclo
     */
    public RelationEntity<? extends Entity> getLoopBeginning() {
        return loopBeginning;
    }

    /**
     * Registra una entidad en el historial de entidades cargadas.
     * 
     * @param entity Entidad a registrar
     */
    protected void registrar(RelationEntity<? extends Entity> entity) {
        historial.add(entity);
    }

    /**
     * Clase interna que representa una entidad relacionada.
     * Contiene la clase de la entidad, su ID y la entidad cargada.
     * 
     * @param <E> Tipo de entidad
     */
    protected static class RelationEntity<E extends Entity> {
        /** Clase de la entidad. */
        private final Class<E> entityClass;
        /** ID de la entidad. */
        private final Long id;
        /** Entidad cargada. Puede ser {@code null} si no se ha cargado aún. */
        private E loadedEntity;

        /**
         * Constructor de la entidad relacionada.
         * 
         * @param entityClass Clase de la entidad
         * @param id ID de la entidad
         */
        public RelationEntity(Class<E> entityClass, Long id) {
            this.entityClass = entityClass;
            this.id = id;
        }

        /** Obtiene la clase de la entidad. */
        public Class<E> getEntityClass() {
            return entityClass;
        }

        /** Obtiene el ID de la entidad. */
        public Long getId() {
            return id;
        }

        /** Obtiene la entidad cargada. Puede ser {@code null} si no se ha cargado aún. */
        public E getLoadedEntity() {
            return loadedEntity;
        }

        /** Establece la entidad cargada. */
        public void setLoadedEntity(E loadedEntity) {
            this.loadedEntity = loadedEntity;
        }

        /** 
         * Compara dos objetos RelationEntity.
         * Dos RelationEntity son iguales si tienen la misma clase de entidad y el mismo ID.
         * @param obj Objeto a comparar
         * @return {@code true} si son iguales, {@code false} en caso contrario
         */
        @Override
        public boolean equals(Object obj) {
            if(this == obj) return true;
            if(!(obj instanceof RelationEntity that)) return false;
            return entityClass.equals(that.entityClass) && id.equals(that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityClass, id);
        }
    }
}
