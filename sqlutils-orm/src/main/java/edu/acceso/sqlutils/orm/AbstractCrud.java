package edu.acceso.sqlutils.orm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.orm.DaoFactory.DaoData;
import edu.acceso.sqlutils.orm.mapper.EntityMapper;
import edu.acceso.sqlutils.orm.mapper.SqlTypesTranslator;
import edu.acceso.sqlutils.orm.minimal.Entity;
import edu.acceso.sqlutils.orm.minimal.crud.MinimalCrudInterface;
import edu.acceso.sqlutils.orm.minimal.sql.MinimalSqlQuery;
import edu.acceso.sqlutils.orm.relations.FetchPlan;
import edu.acceso.sqlutils.orm.relations.RelationLoader;

/** 
 * Clase que implementa las operaciones CRUD para una entidad genérica E.
 * @param <E> Tipo de entidad que extiende {@link Entity}.
 */
public abstract class AbstractCrud<E extends Entity> implements MinimalCrudInterface<E> {
    public static final Logger logger = LoggerFactory.getLogger(AbstractCrud.class);

    /** Datos que caracterizan al {@link DaoFactory} al que pertenece este DAO. */
    protected final DaoData data;
    /** Clase que implementa las sentencias SQL para las operaciones CRUD. */
    protected final MinimalSqlQuery sqlQuery;
    /**
     * Mapper de la entidad E que mapea registros de la base de datos a objetos de tipo E.
     * Este mapper es responsable de convertir filas de ResultSet en instancias de E y viceversa.
     */
    protected final EntityMapper<E> mapper;
    /** Clase de la entidad que maneja este objeto DAO */
    protected final Class<E> entityClass;
    /** Cargador de relaciones que originó este DAO. Es nulo si el DAO no se creo a partir de uno. */
    protected final RelationLoader<? extends Entity> originalLoader;

    /**
     * Constructor que recibe una clave y una clase que implementa {@link MinimalSqlQuery}.
     * @param key La clave que identifica al {@link DaoFactory} que creó este DAO.
     * @param entityClass La clase de la entidad que maneja este CRUD.
     */
    /**
     * Constructor que crea una nueva instancia de {@link AbstractCrud} a partir de una clave.
     * @param key La clave que identifica al {@link DaoFactory} que creó este DAO.
     * @param entityClass La clase de la entidad que maneja este CRUD.
     */
    @SuppressWarnings("unchecked")
    public AbstractCrud(String key, Class<E> entityClass) {
        this.data = DaoFactory.get(key).getDaoData();

        this.entityClass = entityClass;
        this.mapper = (EntityMapper<E>) data.mappers().get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.originalLoader = null;

        this.sqlQuery = data.sqlQueryFactory().createSqlQuery(mapper);
        logger.debug("Creado DAO para '{}' con '{}', con operaciones '{}[{}]' y estrategia de carga '{}'.",
            entityClass.getSimpleName(),
            mapper.getClass().getSimpleName(),
            this.getClass().getSimpleName(),
            sqlQuery.getClass().getSimpleName(),
            data.fetchPlan()
        );
    }

    /**
     * Constructor que crea una nueva instancia de {@link AbstractCrud} a partir de un DAO original y un nuevo {@link FetchPlan}.
     * @param original DAO original del que se copian los parámetros.
     * @param newData Nuevos datos que caracterizarán al nuevo DAO.
     */
    protected AbstractCrud(AbstractCrud<E> original, DaoData newData) {
        this.data = newData;

        this.entityClass = original.entityClass;
        this.mapper = original.mapper;
        this.originalLoader = original.originalLoader;
        this.sqlQuery = original.sqlQuery;
        logger.debug("Creado DAO clonado para '{}' con '{}', con operaciones '{}[{}]' y estrategia de carga '{}'.",
            entityClass.getSimpleName(),
            mapper.getClass().getSimpleName(),
            this.getClass().getSimpleName(),
            sqlQuery.getClass().getSimpleName(),
            data.fetchPlan()
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
     * @param rl {@link RelationLoader} que origina este DAO.
     */
    @SuppressWarnings("unchecked")
    public AbstractCrud(RelationLoader<E> rl) {
        data = rl.getData();
        this.entityClass = rl.getEntityClass();
        mapper = (EntityMapper<E>) data.mappers().get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        sqlQuery = data.sqlQueryFactory().createSqlQuery(mapper);
        originalLoader = rl;
        logger.atTrace()
            .setMessage("Creado DAO de '{}' para cargar entidad foránea.")
            .addArgument(entityClass.getSimpleName())
            .log();
    }

    /**
     * Obtiene la clave que identifica al DaoFactory que creó este DAO.
     * @return La clave solicitada.
     */
    public String getKey() {
        return data.key();
    }

    /**
     * Crea un nuevo DAO con el mismo DaoData pero con un nuevo FetchPlan.
     * @param newData Nuevos datos que caracterizarán al nuevo DAO.
     * @return El nuevo DAO solicitado.
     */
    public abstract AbstractCrud<E> with(DaoData newData);

    /**
     * Crea una instancia de {@link RelationLoader}.
     * @param <T> Tipo de entidad que el RelationLoader se encargará de cargar.
     * @param entityClass Clase de la entidad que el {@link RelationLoader} se encargará de cargar.
     * @return La instancia solicitada.
     * @throws DataAccessException Si ocurre un error al crear la instancia.
     */
    public <T extends Entity> RelationLoader<T> createNewRelationLoader(Class<T> entityClass) throws DataAccessException {
        return originalLoader == null 
            ? new RelationLoader<>(entityClass, data)
            : new RelationLoader<>(entityClass, originalLoader);
    }

    /**
     * Obtiene la clase de la entidad manejada por este objeto DAO.
     * @return La clase de la entidad relacionada.
     */
    public Class<E> getEntityClass() {
        return entityClass;
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
            Class<? extends Entity> entityClass = data.mappers().entrySet()
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