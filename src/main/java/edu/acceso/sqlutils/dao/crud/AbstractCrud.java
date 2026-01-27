package edu.acceso.sqlutils.dao.crud;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.crud.MinimalCrudInterface;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.mapper.SqlTypesTranslator;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.tx.TransactionManager;

/** 
 * Clase que implementa las operaciones CRUD para una entidad genérica T.
 * @param <T> Tipo de entidad que extiende {@link Entity}.
 */
public abstract class AbstractCrud<T extends Entity> implements MinimalCrudInterface<T> {
    public static final Logger logger = LoggerFactory.getLogger(AbstractCrud.class);

    protected final TransactionManager tm;

    /** Clase que implementa las sentencias SQL para las operaciones CRUD. */
    protected final MinimalSqlQuery sqlQuery;
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
     * Constructor que recibe una clave y una clase que implementa {@link MinimalSqlQuery}.
     * @param key Clave que identifica la fuente de datos.
     * @param entityClass La clase de la entidad que maneja este CRUD.
     * @param mappers El EntityMapper que mapea entidades a registros de la base de
     * @param sqlQueryClass La clase que implementa las consultas SQL.
     * @param loaderClass La clase que implementa el cargador de relaciones.
     */
    @SuppressWarnings("unchecked")
    public AbstractCrud(String key, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                        Class<? extends MinimalSqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        this.tm = TransactionManager.get(key);
        this.mappers = mappers;
        this.mapper = (EntityMapper<T>) mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.sqlQuery = createSqlQueryInstance(sqlQueryClass, mapper);
        // TODO:: Revisar esto. Un mismo DAO puede usarse para cargar dos entidades sin relación entre ellas.
        // Por ejemplo, la obtención de dos estudiantes. Estos dos estudiantes comparten el mismo DAO y, por tanto,
        // el mismo RelationLoader, pero no deberían esto último puesto que entonces comparten el historial de entidades cargadas.
        this.loader = createRelationLoaderInstance(loaderClass);
        logger.debug("Creado DAO para '{}' con '{}', con operaciones '{}[{}]' y usando como estrategia para carga de entidades foráneas '{}'.",
            entityClass.getSimpleName(),
            mapper.getClass().getSimpleName(),
            this.getClass().getSimpleName(),
            sqlQueryClass.getSimpleName(),
            loaderClass.getSimpleName()
        );
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
        tm = dao.tm;
        mappers = dao.mappers;
        mapper = (EntityMapper<T>) mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        sqlQuery = createSqlQueryInstance(dao.sqlQuery.getClass(), mapper);
        loader = dao.loader;
        logger.atTrace()
            .setMessage("Creado DAO de '{}' para cargarlo como entidad foránea de una entidad '{}'.")
            .addArgument(entityClass.getSimpleName())
            .addArgument(() -> dao.getEntityClass().getSimpleName())
            .log();
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
    private static <S extends MinimalSqlQuery> S createSqlQueryInstance(Class<S> sqlQueryClass, EntityMapper<? extends Entity> mapper) {
        try {
            return sqlQueryClass.getConstructor(String.class, String.class, String[].class).newInstance(
                mapper.getTableInfo().tableName(),
                mapper.getTableInfo().idColumn().getName(),
                mapper.getTableInfo().getColumnNames()
            );
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error al crear instancia de SqlQuery", e);
        }
    }

    /**
     * Obtiene la clase de la entidad manejada por este objeto DAO.
     * @return La clase de la entidad relacionada.
     */
    public Class<? extends Entity> getEntityClass() {
        return mapper.getEmptyEntity().getClass();
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