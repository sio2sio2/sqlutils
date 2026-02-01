package edu.acceso.sqlutils.dao.relations;

import java.util.Objects;

import edu.acceso.sqlutils.crud.Entity;

/**
 * Clase interna que representa una entidad relacionada.
 * Contiene la clase de la entidad, su ID y la entidad cargada.
 * 
 * @param <E> Tipo de entidad
 */
public class RelationEntity<E extends Entity> {
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

    /** Obtiene la clase de la entidad.
     * @return Clase de la entidad
     */
    public Class<E> getEntityClass() {
        return entityClass;
    }

    /** Obtiene el ID de la entidad.
     * @return ID de la entidad
     */
    public Long getId() {
        return id;
    }

    /** Obtiene la entidad cargada.
     * @return Entidad cargada o {@code null}, si no se ha cargado aún.
     */
    public E getLoadedEntity() {
        return loadedEntity;
    }

    /** Establece la entidad cargada.
     * @param loadedEntity Entidad cargada
     */
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

    /** 
     * Calcula el hashcode de un objeto {@link Entity} basado en su clase y su ID.
     * @param entity Objeto del que calcular el hashcode
     * @return Hashcode calculado
     */
    public static int hash(Entity entity) {
        return new RelationEntity<>(entity.getClass(), entity.getId()).hashCode();
    }

    /** 
     * Calcula el hashcode de una entidad basada en su clase y su ID.
     * @param entityClass Clase de la entidad
     * @param id ID de la entidad
     * @return Hashcode calculado
     */
    public static int hash(Class<? extends Entity> entityClass, Long id) {
        return new RelationEntity<>(entityClass, id).hashCode();
    }
}
