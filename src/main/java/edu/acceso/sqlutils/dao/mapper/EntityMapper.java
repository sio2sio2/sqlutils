package edu.acceso.sqlutils.dao.mapper;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz genérica para operaciones de acceso a datos orientadas a objetos.
 * Permite convertir entidades a parámetros de consulta y viceversa.
 *
 * @param <T> Tipo de entidad que maneja el DAO.
 */
public interface EntityMapper<T extends Entity> {

    /**
     * Obtiene la información de la tabla asociada a la entidad (ver {@link TableInfo}).
     *
     * @return Información de la tabla.
     */
    public TableInfo getTableInfo();

    /**
     * Convierte una entidad a los parámetros necesarios para una consulta SQL.
     *
     * @param pstmt PreparedStatement donde se establecerán los parámetros.
     * @param entity Entidad a convertir.
     * @throws SQLException Si ocurre un error al establecer los parámetros.
     */
    default void EntityToParams(PreparedStatement pstmt, T entity) throws SQLException {
        Column[] columns = getTableInfo().columns();
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            try {
                Field field = entity.getClass().getDeclaredField(column.getField());
                field.setAccessible(true);
                Object value = field.get(entity);
                Class<?> fieldType = column.getFieldType() != null ? column.getFieldType() : field.getType();
                int sqlType;
                if(Column.isForeignKey(fieldType)) {
                    if(value != null) value = ((Entity) value).getId();
                    sqlType = Types.BIGINT;
                }
                else {
                    // Si hay definido un traductor, se utiliza para serializar el valor
                    // y el tipo de dato debe ser el del valor serializado.
                    if(column.getTranslator() != null) {
                        value = column.getTranslator().serialize(value);
                        fieldType = value.getClass();
                    }
                    SqlTypesTranslator translator = new SqlTypesTranslator(fieldType, value);
                    sqlType = translator.getType();
                    value = translator.getSqlValue();
                }
                pstmt.setObject(i + 1, value, sqlType);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Error al acceder al campo: " + column.getField(), e);
            }
        }
        // Establecer el ID de la entidad al final (véase {@link SqlQuery})
        pstmt.setObject(columns.length + 1, entity.getId(), Types.BIGINT);
    }

    /**
     * Obtiene una instancia vacía de la entidad asociada a este mapper.
     * @return La instancia requerida.
     */
    @SuppressWarnings("unchecked")
    default T getEmptyEntity() {
        try {
            return getEntityType((Class<? extends EntityMapper<T>>) this.getClass()).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error al crear una instancia de la entidad. ¿Tiene un constructor sin parámetros?", e);
        }
    }

    /**
     * Convierte un ResultSet a una entidad.
     *
     * @param rs ResultSet que contiene los datos de la entidad.
     * @param dao Cargador de relaciones para entidades relacionadas.
     * @return Entidad convertida.
     * @throws SQLException Si ocurre un error al acceder a los datos.
     */
    @SuppressWarnings("unchecked")
    default T resultSetToEntity(ResultSet rs, AbstractCrud<? extends Entity> dao) throws SQLException {
        T entity = getEmptyEntity();
        entity.setId(rs.getLong(getTableInfo().idColumn().getName()));

        for(Column column : getTableInfo().columns()) {
            try {
                Field field = entity.getClass().getDeclaredField(column.getField());
                field.setAccessible(true);

                Object value = null;
                Class<?> fieldType = column.getFieldType() != null ? column.getFieldType() : field.getType();
                if(Column.isForeignKey(fieldType)) {
                    value = rs.getObject(column.getName(), Long.class);
                    try {
                        RelationLoader<? extends Entity> loader = dao.createNewRelationLoader((Class<? extends Entity>) fieldType);
                        if(value != null) value = loader.loadEntity((Long) value);
                    } catch (DataAccessException e) {
                        throw new SQLException(String.format("Error al cargar la entidad relacionada: %s", column.getField()), e.getCause());
                    }
                } else {
                    value = column.getTranslator() != null
                        ? column.getTranslator().deserialize(value = rs.getObject(column.getName()))
                        : rs.getObject(column.getName(), fieldType);
                }
                field.set(entity, value);

            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Error al acceder al campo: " + column.getField(), e);
            }
        }

        return entity;
    }

    /**
     * Obtiene la clase de la entidad a partir de la clase del objeto {@link EntityMapper} correspondiente.
     * @param entityMapperClass La clase DAO.
     * @param <T> Tipo de la entidad.
     * @return La clase de la entidad para la que se definió la clase DAO.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Entity> Class<T> getEntityType(Class<? extends EntityMapper<T>> entityMapperClass) {
        for(Type genericInterface: entityMapperClass.getGenericInterfaces()) {
            if (genericInterface instanceof ParameterizedType) {
                if (((ParameterizedType) genericInterface).getRawType().equals(EntityMapper.class)) {
                    Type[] typeArguments = ((ParameterizedType) genericInterface).getActualTypeArguments();
                    if (typeArguments.length > -1 && typeArguments[0] instanceof Class<?> typeArg && Entity.class.isAssignableFrom(typeArg)) {
                        return (Class<T>) typeArg;
                    }
                }
            }
        }
        throw new IllegalArgumentException(String.format("La clase '%s' no implementa EntityMapper<T> con un tipo genérico específico.", entityMapperClass.getName()));
    }

}
