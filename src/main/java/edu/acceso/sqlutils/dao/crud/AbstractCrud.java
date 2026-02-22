package edu.acceso.sqlutils.dao.crud;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.crud.MinimalCrudInterface;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.mapper.SqlTypesTranslator;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.dao.tx.Cache;
import edu.acceso.sqlutils.dao.tx.CacheListener;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.tx.LoggingManager;
import edu.acceso.sqlutils.tx.TransactionManager;

/** 
 * Clase que implementa las operaciones CRUD para una entidad genérica E.
 * @param <E> Tipo de entidad que extiende {@link Entity}.
 */
public abstract class AbstractCrud<E extends Entity> implements MinimalCrudInterface<E> {
    public static final Logger logger = LoggerFactory.getLogger(AbstractCrud.class);

    protected final TransactionManager tm;

    /** Clase que implementa las sentencias SQL para las operaciones CRUD. */
    protected final MinimalSqlQuery sqlQuery;
    /**
     * Mapper de la entidad E que mapea registros de la base de datos a objetos de tipo E.
     * Este mapper es responsable de convertir filas de ResultSet en instancias de E y viceversa.
     */
    protected final EntityMapper<E> mapper;
    /**
     * Mappers de entidades.
     */
    protected final Map<Class<? extends Entity>, EntityMapper<?>> mappers;

    /** Clase que implementa los cargadores de relaciones. */
    protected final Class<? extends RelationLoader<? extends Entity>> loaderClass;

    /** Cargador de relaciones que originó este DAO. Es nulo si el DAO no se creo a partir de uno. */
    protected final RelationLoader<? extends Entity> originalLoader;

    /** Clase de la entidad que maneja este objeto DAO */
    protected final Class<E> entityClass;

    /**
     * Constructor que recibe una clave y una clase que implementa {@link MinimalSqlQuery}.
     * @param key Clave que identifica la fuente de datos.
     * @param entityClass La clase de la entidad que maneja este CRUD.
     * @param mappers El EntityMapper que mapea entidades a registros de la base de datos.
     * @param sqlQueryClass La clase que implementa las consultas SQL.
     * @param loaderClass La clase que implementa el cargador de relaciones.
     */
    @SuppressWarnings("unchecked")
    public AbstractCrud(String key, Class<E> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                        Class<? extends MinimalSqlQuery> sqlQueryClass, Class<? extends RelationLoader<? extends Entity>> loaderClass) {
        this.tm = TransactionManager.get(key);
        this.mappers = mappers;
        this.entityClass = entityClass;
        this.mapper = (EntityMapper<E>) mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.sqlQuery = createSqlQueryInstance(sqlQueryClass, mapper);
        this.loaderClass = loaderClass;
        this.originalLoader = null;
        logger.debug("Creado DAO para '{}' con '{}', con operaciones '{}[{}]' y usando como estrategia para carga de entidades foráneas '{}'.",
            entityClass.getSimpleName(),
            mapper.getClass().getSimpleName(),
            this.getClass().getSimpleName(),
            sqlQueryClass.getSimpleName(),
            loaderClass.getSimpleName()
        );
    }

    /**
     * Constructor que crea una nueva instancia de {@link AbstractCrud} a partir de un objeto {@link RelationLoader}.
     * 
     * <p>
     * Este objeto {@link AbstractCrud} se construye compartiendo los mismos parámeros que el DAO original
     * que creó el {@link RelationLoader} que se le pasa como argumento. Esto permite conocer
     * cuál es el historial de entidades cargadas y evitar ciclos de referencia.
     * </p>
     * @param originalDao DAO original a partir del cual se crea este nuevo DAO.
     * @param rl {@link RelationLoader} que origina este DAO.
     */
    @SuppressWarnings("unchecked")
    public AbstractCrud(AbstractCrud<? extends Entity> originalDao, RelationLoader<E> rl) {
        tm = originalDao.tm;
        mappers = originalDao.mappers;
        this.entityClass = rl.getEntityClass();
        mapper = (EntityMapper<E>) mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        sqlQuery = createSqlQueryInstance(originalDao.sqlQuery.getClass(), mapper);
        loaderClass = originalDao.loaderClass;
        originalLoader = rl;
        logger.atTrace()
            .setMessage("Creado DAO de '{}' para cargar entidad foránea.")
            .addArgument(entityClass.getSimpleName())
            .log();
    }

    /**
     * Crea una instancia de {@link RelationLoader}.
     * @param <T> Tipo de entidad que el RelationLoader se encargará de cargar.
     * @param entityClass Clase de la entidad que el {@link RelationLoader} se encargará de cargar.
     * @return La instancia solicitada.
     * @throws DataAccessException Si ocurre un error al crear la instancia.
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> RelationLoader<T> createNewRelationLoader(Class<T> entityClass) throws DataAccessException {
        try {
            return originalLoader == null
                 ? (RelationLoader<T>) loaderClass.getConstructor(AbstractCrud.class, Class.class).newInstance(this, entityClass)
                 : (RelationLoader<T>) loaderClass.getConstructor(RelationLoader.class, Class.class).newInstance(originalLoader, entityClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error al crear instancia de RelationLoader", e);
        }
    }

    /**
     * Obtiene un DAO con las mismas características que el DAO original,
     * pero para otra tipo de entidad. Los cargadores de arranque que puede
     * crear esta entidad tiene como cargador previo el cargador que originó este DAO.
     * @param <T> Clase de la entidad que el cargador se encargará de cargar.
     * @param rl Cargador de relaciones que originará el nuevo DAO
     * @return El DAO solicitado.
     * @throws DataAccessException Si ocurre un error al generar el DAO
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> AbstractCrud<T> createLinkedDao(RelationLoader<T> rl) throws DataAccessException {
        try {
            return getClass().getDeclaredConstructor(getClass(), RelationLoader.class)
                .newInstance(this, rl);
        } catch (ReflectiveOperationException e) {
            throw new DataAccessException(String.format("No se pudo obtener el DAO para la entidad %s", rl.getEntityClass().getSimpleName()), e);
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
    public Class<E> getEntityClass() {
        return entityClass;
    }

    /**
     * Verifica que la transacción esté activa.
     * @throws IllegalStateException Si el gestor de transacciones no tiene una transacción abierta.
     */
    protected void checkTransactionActive() {
        if (!tm.isActive()) throw new IllegalStateException("El gestor de transacciones '%s' no tiene una transacción abierta.".formatted(tm.getKey()));
    }

    /**
     * Almacena la entidad en la caché de la transacción actual.
     * @param entity Entidad a almacenar.
     */
    protected E putInCache(E entity) {
        getCache().put(entity);
        return entity;
    }   

    /**
     * Elimina la entidad de la caché de la transacción actual.
     * @param id ID de la entidad a eliminar.
     * @return Entidad eliminada de la caché o {@code null} si no existía.
     */
    protected E deleteFromCache(Long id) {
        return getCache().delete(getEntityClass(), id);
    }

    /**
     * Obtiene la caché de la transacción actual.
     * @return La caché de la transacción actual.
     */
    public Cache getCache() {
        CacheListener cacheManager = TransactionManager.get(tm.getKey()).getContext().getEventListener(CacheListener.KEY, CacheListener.class);
        return cacheManager.getCache();
    }

    /**
     * Permite pasar mensajes de registros que se difieren hasta que se conozca el resultado de la transacción (commit o rollback).
     * @param name El nombre del logger que se utilizará para registrar el mensaje.
     * @param level El nivel de log (INFO, ERROR, etc.) para el mensaje.
     * @param successMessage El mensaje que se registrará si la transacción se confirma (commit).
     * @param failMessage El mensaje que se registrará si la transacción se revierte (rollback).
     */
    protected void sendLogMessage(String name, Level level, String successMessage, String failMessage) {
        LoggingManager loggerManager = TransactionManager.get(tm.getKey()).getContext().getEventListener(LoggingManager.KEY, LoggingManager.class);
        loggerManager.sendMessage(name, level, successMessage, failMessage);
    }

    /**
     * Permite pasar mensajes de registros que se difieren hasta que se conozca el resultado de la transacción (commit o rollback).
     * @param clazz La clase que es responsable de generar el mensaje.
     * @param level El nivel de log (INFO, ERROR, etc.) para el mensaje.
     * @param successMessage El mensaje que se registrará si la transacción se confirma (commit).
     * @param failMessage El mensaje que se registrará si la transacción se revierte (rollback).
     */
    protected void sendLogMessage(Class<?> clazz, Level level, String successMessage, String failMessage) {
        sendLogMessage(clazz.getName(), level, successMessage, failMessage);
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