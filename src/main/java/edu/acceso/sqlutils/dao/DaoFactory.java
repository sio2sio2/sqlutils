package edu.acceso.sqlutils.dao;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.ConnectionPool;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.dao.crud.DaoProvider;
import edu.acceso.sqlutils.dao.crud.simple.SimpleCrudInterface;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.LoaderFactory;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.tx.TransactionManager;

/** 
 * Fábrica de DAOs que permite crear objetos DAO para realizar operaciones CRUD
 * sobre entidades. Esta clase es responsable de registrar los mappers de entidades y
 * proporcionar acceso a ellas a través del método {@link #getDao(Class)}.
 * 
 */
public class DaoFactory implements AutoCloseable {
    /** Logger para registrar información y errores. */
    private static final Logger logger = LoggerFactory.getLogger(DaoFactory.class);

    /** Clave para la caché de entidades en el gestor de transacciones */
    public static final String CACHE_RESOURCE_KEY = new Object().toString();

    /**
     * Instancias de DaoFactory para implementar un patrón Multiton.
     */
    private static final Map<String, DaoFactory> instances = new ConcurrentHashMap<>();

    /** Mapa que relaciona las clases de entidad con sus mappers. */
    private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers;
    /** Pool de conexiones asociado a esta fábrica de DAOs. */
    private final ConnectionPool cp;
    /** Proveedor de DAOs (que proporciona una implementación de las operaciones CRUD y una definición de las consultas SQL) */
    private final DaoProvider daoProvider;
    /** Clase que implementa el cargador de relaciones. */
    private final Class<? extends RelationLoader<? extends Entity>> loaderClass;

    /**
     * Constructor privado para la fábrica de DAOs.
     * @param cp Pool de conexiones que utiliza la fábrica de DAOs.
     * @param daoProvider Proveedor de DAOs 
     * @param loaderClass Clase que implementa el cargador de relaciones.
     * @param mappers Mapa de mappers de entidades.
     */
    private DaoFactory(ConnectionPool cp, DaoProvider daoProvider, Class<? extends RelationLoader<? extends Entity>> loaderClass, Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers) {
        this.cp = cp;
        cp.setTransactionManager(DaoFactory::addCacheToTransactionManager);
        this.daoProvider = daoProvider;
        this.loaderClass = loaderClass;
        this.mappers = mappers;
    }

    /**
     * Agrega una caché de entidades al gestor de transacciones
     * @param tm El gestor de transacciones al que se le añadirá la caché.
     */
    private static void addCacheToTransactionManager(TransactionManager tm) {
        tm.addActionOnAllBegin(new Consumer<TransactionManager>() {
            @Override
            public void accept(TransactionManager tm) {
                tm.getResources().put(CACHE_RESOURCE_KEY, new Cache());
                logger.trace("Creada nueva caché de entidades para la trasacción");
            }
        });

        // No hace falta eliminar la caché, porque se borran todos los recursos al cerrar la transacción.
    }

    /**
     * Clase que permite construir instancias de {@link DaoFactory}.
     */
    public static class Builder {
        /** Clave que identifica la conexión */
        private final String key;

        /**
         * Mapa de mappers de entidades.
         */
        private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers = new ConcurrentHashMap<>();
        /**
         * Proveedor de DAOs.
         */
        private final DaoProvider daoProvider;

        /**
         * Constructor privado para la clase Builder.
         * @param daoProvider Proveedor de DAOs.
         */
        private Builder(String key, DaoProvider daoProvider) {
            this.key = key;
            this.daoProvider = daoProvider;
        }

        /**
         * Crea una nueva instancia de {@link DaoFactory.Builder}.
         * @param key Clave que identifica la conexión.
         * @param daoProvider Proveedor de DAOs.
         * @return Una nueva instancia de {@link DaoFactory.Builder}.
         */
        public static Builder create(String key, DaoProvider daoProvider) {
            return new Builder(key, daoProvider);
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
         * Genera una nueva instancia de {@link DaoFactory}.
         * @param loader Fabrica de cargadores de relaciones.
         * @param dbUrl URL de la base de datos.
         * @return Una nueva instancia de {@link DaoFactory}.
         */
        public DaoFactory get(LoaderFactory loader, String dbUrl) {
            return get(loader, dbUrl, null, null);
        }

        /**
         * Genera una nueva instancia de {@link DaoFactory}.
         * @param loader Fabrica de cargadores de relaciones.
         * @param dbUrl URL de la base de datos.
         * @param user Usuario de acceso de la base de datos.
         * @param password Contraseña del usuario.
         * @return Una nueva instancia de {@link DaoFactory}.
         */
        public DaoFactory get(LoaderFactory loader, String dbUrl, String user, String password) {

            return instances.computeIfAbsent(key, k -> {
                @SuppressWarnings("unchecked")
                var loaderClass = (Class<? extends RelationLoader<? extends Entity>> ) loader.getLoaderClass();
	    
                ConnectionPool cp;
                try {
                    cp = ConnectionPool.create(key, dbUrl, user, password);
                } catch(IllegalStateException e) {  // Creado fuera de la fábrica: lo recuperamos
                    cp = ConnectionPool.get(key);
                }

                return new DaoFactory(cp, daoProvider, loaderClass, new HashMap<>(mappers));
            });
        }
    }

    /**
     * Obtiene un objeto {@link SimpleCrudInterface} que permite realizar operaciones CRUD sobre una entidad específica.
     * @param <T> El tipo de la entidad.
     * @param entityClass La clase de la entidad para la que se desea obtener el DAO.
     * @return Un objeto DAO para la entidad especificada.
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> AbstractCrud<T> getDao(Class<T> entityClass) {
        try {
            return (AbstractCrud<T>) daoProvider.getCrudClass().getConstructor(String.class, Class.class, Map.class, Class.class, Class.class)
                    .newInstance(cp.getKey(), entityClass, mappers, daoProvider.getSqlQueryClass(), loaderClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(String.format("Error al crear la instancia de %s", daoProvider.getCrudClass().getSimpleName()), e);
        }
    }

    /**
     * Obtiene el gestor de transacciones para DAOs.
     * @return Gestor de transacciones para DAOs.
     */
    public TransactionManager getTransactionManager() {
        return cp.getTransactionManager();
    }

    /**
     * Obtiene la clave que identifica la base de datos
     * @return La clave solicitada.
     */
    public String getKey() {
        return cp.getKey();
    }

    /**
     * Comprueba si el objeto está abierto
     * @return {@code true} si está abierto.
     */
    public boolean isOpen() {
        return cp.isOpen();
    }

    @Override
    public void close() {
        if(instances.remove(getKey(), this)) {
            cp.close();
            logger.debug("Borrado DaoFactory asociado a la clave {}", getKey());
        }
    }
}
