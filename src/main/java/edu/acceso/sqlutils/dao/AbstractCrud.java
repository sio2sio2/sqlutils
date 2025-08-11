package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.util.Map;

import javax.sql.DataSource;

import edu.acceso.sqlutils.ConnProvider;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.crud.MinimalCrudInterface;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.query.SqlQuery;
import edu.acceso.sqlutils.query.SqlTypesTranslator;

/** 
 * Clase que implementa las operaciones CRUD para una entidad genérica T.
 * @param <T> Tipo de entidad que extiende {@link Entity}.
 * 
 * <p>
 * Puede crearse a partir de una {@link DataSource} o una {@link Connection}.
 * En el primer caso, cada operación CRUD se ejecuta con una conexión nueva del pool.
 * En el segundo, se utiliza una conexión existente y todas las operaciones CRUD la comparten.
 * Esto es útil para transacciones que requieren múltiples operaciones CRUD.
 * </p>
 */
public abstract class AbstractCrud<T extends Entity> implements MinimalCrudInterface<T> {
    /** DAO que proporciona acceso a datos relacionados. */
    protected final ConnProvider cp;
    /** Clase que implementa las sentencias SQL para las operaciones CRUD. */
    protected final SqlQuery sqlQuery;
    /**
     * Mapper de la entidad T que mapea registros de la base de datos a objetos de tipo T.
     * Este mapper es responsable de convertir filas de ResultSet en instancias de T y viceversa.
     */
    protected final EntityMapper<T> mapper;
    /**
     * Mappers de entidades.
     */
    protected final Map<Class<? extends Entity>, EntityMapper<?>> mappers;
    /**
     * Cargador de relaciones que se utiliza para cargar entidades relacionadas.
     * Este cargador es responsable de manejar las relaciones entre entidades y cargar datos adicionales según sea necesario.
     */
    protected final RelationLoader loader;

    /**
     * Constructor que recibe un {@link DataSource} y una clase que implementa {@link SqlQuery}.
     * @param ds Una fuente de datos para obtener conexiones a la base de datos.
     * @param mapper El EntityMapper que mapea entidades a registros de la base de
     * @param daoF La fábrica de DAOs que se utilizará para obtener entidades relacionadas.
     */
    @SuppressWarnings("unchecked")
    public AbstractCrud(DataSource ds, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                        Class<? extends SqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        this.cp = new ConnProvider(ds);
        this.mappers = mappers;
        this.mapper = (EntityMapper<T>) mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.sqlQuery = createSqlQueryInstance(sqlQueryClass, mapper);
        this.loader = createRelationLoaderInstance(loaderClass);
    }

    /**
     * Constructor que recibe una {@link Connection} y una clase que implementa {@link SqlQuery}.
     * @param conn Una conexión a la base de datos.
     * @param mapper El EntityMapper que mapea entidades a registros de la base de
     * @param daoF La fábrica de DAOs que se utilizará para obtener entidades relacionadas.
     */
    @SuppressWarnings("unchecked")
    public AbstractCrud(Connection conn, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                        Class<? extends SqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        this.cp = new ConnProvider(conn);
        this.mappers = mappers;
        this.mapper = (EntityMapper<T>) mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.sqlQuery = createSqlQueryInstance(sqlQueryClass, mapper);
        this.loader = createRelationLoaderInstance(loaderClass);
    }

    /**
     * Constructor que crea una nueva instancia de {@link AbstractCrud} a partir de otro {@link AbstractCrud}.
     * 
     * <p>
     * Un {@link AbstractCrud} obtenido de este modo comparte el cargador de relaciones (véase
     * {@link RelationLoader}) con el original, lo que permite que éste conserve el historial
     * de todas las relaciones cargadas.
     * </p>
     * @param <E> Tipo de entidad del {@link AbstractCrud} original.
     * @param dao El {@link AbstractCrud} original a partir del cual se obtiene el nuevo.
     * @param entityClass La clase de la entidad que maneja el nuevo {@link AbstractCrud}.
     */
    @SuppressWarnings("unchecked")
    public <E extends Entity> AbstractCrud(AbstractCrud<E> dao, Class<T> entityClass) {
        cp = dao.cp;
        mappers = dao.mappers;
        mapper = (EntityMapper<T>) mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        sqlQuery = createSqlQueryInstance(dao.sqlQuery.getClass(), mapper);
        loader = dao.loader;
    }

    /**
     * Crea una instancia de RelationLoader utilizando la clase proporcionada.
     * @param loaderClass La clase que implementa RelationLoader.
     * @return Una instancia de RelationLoader.
     */
    private RelationLoader createRelationLoaderInstance(Class<? extends RelationLoader> loaderClass) {
        try {
            return loaderClass.getConstructor(AbstractCrud.class)
                .newInstance(this);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error al crear instancia de RelationLoader", e);
        }
    }

    /**
     * Crea una instancia de SqlQuery utilizando la clase proporcionada.
     * @param sqlQueryClass La clase que implementa SqlQuery.
     * @return Una instancia de SqlQuery.
     */
    private static SqlQuery createSqlQueryInstance(Class<? extends SqlQuery> sqlQueryClass, EntityMapper<? extends Entity> mapper) {
        try {
            return sqlQueryClass.getConstructor(String.class, String.class, String[].class).newInstance(
                mapper.getTableInfo().tableName(),
                mapper.getTableInfo().idColumn().name(),
                mapper.getTableInfo().getColumnNames()
            );
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error al crear instancia de SqlQuery", e);
        }
    }

    /**
     * Obtiene el traductor de tipos SQL para un campo y un valor dado.
     * El método permite saber cómo traducir a SQL el valor de un campo
     * al usar {@link #get(String, Object)}.
     * @param field El campo para el que se obtiene el traductor.
     * @param value El valor para el que se obtiene el traductor.
     * @return Un traductor de tipos SQL para el campo y el valor dados.
     */
    protected SqlTypesTranslator getTranslator(String field, Object value) {
        Class<?> attrClass = mapper.getTableInfo().getFieldType(field);
        // No se incluyó la información del tipo en el mapper,
        // por lo que hay que averiguarlo por reflexión.
        if(attrClass == null) {
            Class<? extends Entity> entityClass = mappers.entrySet()
                .stream()
                .filter(e -> e.getValue().getClass().isAssignableFrom(mapper.getClass()))
                .map(e -> e.getKey())
                .findFirst().orElse(null);
            assert entityClass != null: "No se encontró la clase de entidad asociada al mapper";
            try {
                attrClass = entityClass.getDeclaredField(field).getType();
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(String.format("El campo %s no existe en la entidad %s", field, entityClass.getSimpleName()), e);
            }
        }
        return (Entity.class.isAssignableFrom(attrClass))
            ? new SqlTypesTranslator(Long.class, ((Entity) value).getId())
            : new SqlTypesTranslator(attrClass, value);
    }
}