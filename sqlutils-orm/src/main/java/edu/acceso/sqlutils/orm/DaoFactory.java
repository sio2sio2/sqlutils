package edu.acceso.sqlutils.orm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.DbmsSelector;
import edu.acceso.sqlutils.jdbc.JdbcConnection;
import edu.acceso.sqlutils.jdbc.DataSourceFactory;
import edu.acceso.sqlutils.jdbc.tx.TransactionManager;
import edu.acceso.sqlutils.orm.mapper.EntityMapper;
import edu.acceso.sqlutils.orm.minimal.Entity;
import edu.acceso.sqlutils.orm.relations.FetchPlan;
import edu.acceso.sqlutils.orm.relations.RelationLoader;
import edu.acceso.sqlutils.orm.simple.crud.SimpleCrudInterface;
import edu.acceso.sqlutils.orm.simple.query.SimpleSqlQuery;
import edu.acceso.sqlutils.orm.simple.query.SimpleSqlQueryGeneric;
import edu.acceso.sqlutils.orm.tx.CacheManager;
import edu.acceso.sqlutils.tx.event.LoggingManager;

/** 
 * Fábrica de DAOs que permite crear objetos DAO para realizar operaciones CRUD
 * sobre entidades. Esta clase es responsable de registrar los mappers de entidades y
 * proporcionar acceso a ellas a través del método {@link #getDao(Class)}.
 * 
 */
public class DaoFactory implements AutoCloseable {
    /** Logger para registrar información y errores. */
    private static final Logger logger = LoggerFactory.getLogger(DaoFactory.class);

    /**
     * Instancias de DaoFactory para implementar un patrón Multiton.
     */
    private static final Map<String, DaoFactory> instances = new ConcurrentHashMap<>();

    /** Mapa que relaciona las clases de entidad con sus mappers. */
    private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers;
    /** Pool de conexiones asociado a esta fábrica de DAOs. */
    /** Fábrica que permite obtener las sentencias SQL apropiadas */
    private final SqlQueryFactory sqlQueryFactory;
    /** Clase que implementa las operaciones CRUD */
    private final Class<? extends AbstractCrud<?>> crudClass;
    private final JdbcConnection jc;
    /** Plan predefinido para la carga de relaciones */
    private final FetchPlan fetchPlan;

    /** Listener para posponer el registro de mensajes hasta que culmine la transacción */
    private final LoggingManager logManager = new LoggingManager();

    /** Listener para implementar una caché de entidades */
    private final CacheManager cacheManager = new CacheManager();

    /**
     * Registro de todos los datos necesarios para crear objetos DAO
     * @param key Clave que identifica la conexión.
     * @param jc Pool de conexiones que utiliza la fábrica de DAOs.
     * @param tm Gestor de transacciones que utiliza la fábrica de DAOs.
     * @param sqlQueryFactory Fábrica de consultas SQL que utiliza la fábrica de DAOs.
     * @param crudClass Clase que implementa las operaciones CRUD.
     * @param fetchPlan Plan predefinido para la carga de relaciones.
     * @param mappers Mapa con los mapeadores de las entidades que constituyen la base de datos.
     */
    public static record DaoData(
        String key,
        JdbcConnection jc,
        TransactionManager tm,
        SqlQueryFactory sqlQueryFactory,
        Class<? extends AbstractCrud<?>> crudClass,
        FetchPlan fetchPlan,
        Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers
    ) {

        /**
         * Define un nuevo registro con otro fetchPlan y el resto de datos iguales.
         * @param fetchPlan Nuevo plan predefinido para la carga de relaciones.
         * @return Un nuevo registro con el fetchPlan especificado y el resto de datos iguales.
         */
        public DaoData with(FetchPlan fetchPlan) {
            return new DaoData(key, jc, tm, sqlQueryFactory, crudClass, fetchPlan, mappers);
        }

        /**
         * Obtiene el gestor de logging para poder posponer los mensajes
         * de log hasta que se conozca el resultado de la transacción.
         * @return El gestor de logging solicitado.
         */
        public LoggingManager loggingManager() {
            TransactionManager tm = jc.getTransactionManager();
            return tm.getContext().getEventListener(LoggingManager.KEY, LoggingManager.class);
        }

        /**
         * Obtiene el gestor de caché para poder cachear las entidades obtenidas de la base de datos
         * durante la transacción.
         * @return El gestor de caché solicitado.
         */
        public CacheManager cacheManager() {
            TransactionManager tm = jc.getTransactionManager();
            return tm.getContext().getEventListener(CacheManager.KEY, CacheManager.class);
        }
    }

    /**
     * Constructor privado para la fábrica de DAOs.
     * @param jc Pool de conexiones que utiliza la fábrica de DAOs.
     * @param sqlQueryFactory Fábrica de consultas SQL que utiliza la fábrica de DAOs.
     * @param crudClass Clase que implementa las operaciones CRUD.
     * @param fetchPlan Plan predefinido para la carga de relaciones.
     * @param mappers Mapa de mappers de entidades.
     */
    private DaoFactory(JdbcConnection jc, SqlQueryFactory sqlQueryFactory, Class<? extends AbstractCrud<?>> crudClass, FetchPlan fetchPlan, Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers) {
        this.jc = jc;
        /** Gestor de transacciones con soporte para logging y cacheo */
        jc.withTransactionManager(Map.of(
            LoggingManager.KEY, logManager,
            CacheManager.KEY, cacheManager)
        );
        this.sqlQueryFactory = sqlQueryFactory;
        this.crudClass = crudClass;
        this.fetchPlan = fetchPlan;
        this.mappers = mappers;
    }

    /**
     * Obtiene un objeto {@link DaoData} que contiene toda la información necesaria relativa a esta factoría
     * para crear objetos DAO.
     * @return La información de esta factoría.
     */
    public DaoData getDaoData() {
        return new DaoData(getKey(), jc, jc.getTransactionManager(), sqlQueryFactory, crudClass, fetchPlan, new HashMap<>(mappers));
    }

    /**
     * Clase que permite construir instancias de {@link DaoFactory}.
     */
    public static class Builder {
        /** Clave que identifica la conexión */
        private final String key;
        /**
         * Fábrica de consultas SQL.
         */
        private final SqlQueryFactory.Builder<?> sqlQueryFactoryBuilder;
        /**
         * Clase que implmenta las operaciones CRUD.
         */
        private final Class<? extends AbstractCrud<?>> crudClass;
        /**
         * Plan predefinido de carga de relaciones.
         */
        private FetchPlan fetchPlan = FetchPlan.EAGER;
        /**
         * Fábrica que permite crear el pool de conexiones. Si no se proporciona, se deja {@code null}
         * y se espera que {@link JdbcConnection} use una implementación por defecto.
         */
        private DataSourceFactory dsFactory = null;
        /**
         * Mapa de mappers de entidades.
         */
        private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers = new ConcurrentHashMap<>();

        /**
         * Constructor privado para la clase Builder.
         * @param key Clave que identifica la conexión.
         * @param crudClass Clase que implementa el CRUD.
         * @param sqlQueryFactoryBuilder Constructor de la fábrica de consultas SQL.
         */
        private Builder(String key, Class<? extends AbstractCrud<?>> crudClass, SqlQueryFactory.Builder<?> sqlQueryFactoryBuilder) {
            this.key = key;
            this.sqlQueryFactoryBuilder = sqlQueryFactoryBuilder;
            this.crudClass = crudClass;
        }

        /**
         * Crea una nueva instancia de {@link DaoFactory.Builder}.
         * @param <C> Tipo de clase que implementa el CRUD.
         * @param key Clave que identifica la conexión.
         * @param crudClass Clase que implementa el CRUD.
         * @param sqlQueryFactoryBuilder Constructor de la fábrica de consultas SQL.
         * @return Una nueva instancia de {@link DaoFactory.Builder}.
         */
        public static <C extends AbstractCrud<?>>  Builder create(String key, Class<C> crudClass, SqlQueryFactory.Builder<?> sqlQueryFactoryBuilder) {
            return new Builder(key, crudClass, sqlQueryFactoryBuilder);
        }

        /**
         * Crea una nueva instancia de {@link DaoFactory.Builder} sin especificar un constructor para la fábrica de consultas SQL.
         * En este caso, se usa la implementación genérica {@link SimpleSqlQueryGeneric} que es la única que proporciona la librería
         * y es compatible con cualquier SGBD y las operaciones CRUD definidas en {@link SimpleCrudInterface}.
         * @param <C> Tipo de clase que implementa las operaciones CRUD.
         * @param key Clave que identifica la conexión.
         * @param crudClass Clase que implementa las operaciones CRUD.
         * @return Una nueva instancia de {@link DaoFactory.Builder}.
         */
        public static <C extends AbstractCrud<?>>  Builder create(String key, Class<C> crudClass) {
            return new Builder(key, crudClass, null);
        }

        /**
         * Registra un {@link EntityMapper} para una entidad específica.
         * @param <T> Tipo de entidad.
         * @param entityMapperClass La clase del objeto {@link EntityMapper} que se va a registrar.
         * @return La propia instancia de {@link DaoFactory} para permitir el encadenamiento de llamadas.
         */
        public <T extends Entity> Builder registerMapper(Class<? extends EntityMapper<T>> entityMapperClass) {
            Class<T> entityClass = EntityMapper.getEntityType(entityMapperClass);
            EntityMapper<T> mapper = null;
            try {
                mapper = entityMapperClass.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Error al crear instancia de '%s'".formatted(entityMapperClass.getSimpleName()), e);
            }
            mappers.put(entityClass, mapper);
            logger.trace("Registrado mapper '{}'' para la entidad '{}'", entityMapperClass.getSimpleName(), entityClass.getSimpleName());
            return this;
        }

        /**
         * Define cuál debe ser el plan predefinido para la carga de relaciones
         * @param fetchPlan Plan predefinido para la carga de relaciones
         * @return El propio objeto para seguir encadenado la configuración
         */
        public Builder with(FetchPlan fetchPlan) {
           this.fetchPlan = fetchPlan;
           return this;
        }

        /**
         * Define la fábrica que se utilizará para crear el pool de conexiones asociado a esta factoría de DAOs.
         * @param dsFactory La fábrica que se desea utilizar.
         * @return El propio objeto para seguir encadenado la configuración.
         */
        public Builder with(DataSourceFactory dsFactory) {
            this.dsFactory = dsFactory;
            return this;
        }

        /**
         * Obtiene el constructor de fábrica de consultas SQL y, si no se facilitó
         * ninguno, se crea uno nuevo con la implementación genérica {@link SimpleSqlQueryGeneric} para cualquier SGBD.
         * @return El constructor de fábrica de consultas SQL solicitado.
         */
        private SqlQueryFactory.Builder<?> getSqlQueryFactoryBuilder() {
            if(sqlQueryFactoryBuilder != null) return sqlQueryFactoryBuilder;
            return SqlQueryFactory.Builder.create(SimpleSqlQuery.class)
                .register("*", SimpleSqlQueryGeneric.class);
        }
        
        /**
         * Genera una nueva instancia de {@link DaoFactory}.
         * @param dbUrl URL de la base de datos.
         * @param user Usuario de acceso de la base de datos.
         * @param password Contraseña del usuario.
         * @return Una nueva instancia de {@link DaoFactory}.
         */
        public DaoFactory get(String dbUrl, String user, String password) {

            return instances.computeIfAbsent(key, k -> {
                JdbcConnection cp;
                try {
                    cp = JdbcConnection.create(key, dbUrl, user, password, dsFactory);
                } catch(IllegalStateException e) {  // Creado fuera de la fábrica: lo recuperamos
                    cp = JdbcConnection.get(key);
                }

                SqlQueryFactory sqlQueryFactory = getSqlQueryFactoryBuilder().get(DbmsSelector.fromUrl(dbUrl));
                return new DaoFactory(cp, sqlQueryFactory, crudClass, fetchPlan, new HashMap<>(mappers));
            });
        }
    }

    /**
     * Obtiene la instancia de {@link DaoFactory} asociada a la clave especificada.
     * @param key La clave que identifica la instancia de {@link DaoFactory} que se desea obtener.
     * @return La instancia solicitada.
     */
    public static DaoFactory get(String key) {
        var factory = instances.get(key);
        if(factory == null) {
            throw new IllegalStateException("No existe ningún DaoFactory registrado con la clave '%s'".formatted(key));
        }
        return factory;
    }

    /**
     * Obtiene un objeto DAO que permite realizar operaciones CRUD sobre una entidad específica.
     * @param <T> El tipo de la entidad.
     * @param entityClass La clase de la entidad para la que se desea obtener el DAO.
     * @return Un objeto DAO para la entidad especificada.
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> AbstractCrud<T> getDao(Class<T> entityClass) {
        try {
            return (AbstractCrud<T>) crudClass.getConstructor(String.class, Class.class)
                    .newInstance(getKey(), entityClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(String.format("Error al crear la instancia de %s", crudClass.getSimpleName()), e);
        }
    }

    /**
     * Obtiene un objeto DAO que permite realizar operaciones CRUD sobre una entidad específica.
     * @param <T> El tipo de la entidad.
     * @param rl Cargador de relaciones que origina el DAO que se desea obtener.
     * @return Un objeto DAO para la entidad especificada.
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> AbstractCrud<T> getDao(RelationLoader<T> rl) {
        try {
            return (AbstractCrud<T>) crudClass.getConstructor(RelationLoader.class)
                    .newInstance(rl);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(String.format("Error al crear la instancia de %s", crudClass.getSimpleName()), e);
        }
    }    

    /**
     * Obtiene el gestor de transacciones para DAOs.
     * @return Gestor de transacciones para DAOs.
     */
    public TransactionManager getTransactionManager() {
        return jc.getTransactionManager();
    }

    /**
     * Obtiene la clave que identifica la base de datos
     * @return La clave solicitada.
     */
    public String getKey() {
        return jc.getKey();
    }

    /**
     * Comprueba si el objeto está abierto
     * @return {@code true} si está abierto.
     */
    public boolean isOpen() {
        return jc.isOpen();
    }

    @Override
    public void close() {
        if(instances.remove(getKey(), this)) {
            jc.close();
            logger.debug("Borrado DaoFactory asociado a la clave {}", getKey());
        }
    }
}
