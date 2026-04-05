package edu.acceso.sqlutils.jdbc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.internal.BaseConnection;
import edu.acceso.sqlutils.jdbc.tx.TransactionManager;

/**
 * Pool de conexiones para manejar múltiples conexiones a una base de datos.
 * Utiliza un patrón Multiton para garantizar que solo haya una instancia
 * por URL de conexión, usuario y contraseña.
 */
public class JdbcConnection extends BaseConnection<TransactionManager> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcConnection.class);

    /** Fábrica por defecto para crear DataSources: se busca en el classpath */
    private static final DataSourceFactory DEFAULT_DS_FACTORY = ServiceLoader.load(DataSourceFactory.class)
        .findFirst()
        .orElse(null);

    /** Mapa con las instancias creadas **/
    private static final Map<String, JdbcConnection> instances = new ConcurrentHashMap<>();

    /**
     * El DataSource que se usará para obtener conexiones JDBC en esta instancia de pool de conexiones.
     */
    private final DataSource ds;

    /**
     * Constructor privado para crear una nueva instancia de {@link JdbcConnection}.
     * @param key Clave que identifica esta conexión.
     * @param dbUrl URL de conexión a la base de datos.
     * @param user Usuario de conexión.
     * @param password Contraseña de conexión.
     * @param dsFactory Fábrica de DataSource.
     */
    private JdbcConnection(String key, String dbUrl, String user, String password, DataSourceFactory dsFactory) {
        super(key);
        ds = dsFactory.create(dbUrl, user, password);
    }

    /**
     * Crea una instancia a partir de los datos de conexión.
     * @param key Clave que identifica la conexión.
     * @param dbUrl URL de conexión a la base de datos.
     * @param user Usuario de conexión
     * @param password Contraseña de conexión
     * @param dsFactory Objeto que sabe crear el DataSource.
     * @return La instancia solicitada.
     * @throws IllegalStateException Si la instancia ya existe.
     * @throws IllegalArgumentException Si no hay DataSourceFactory disponible ni se ha proporcionado ninguno como argumento.
     */
    public static JdbcConnection create(String key, String dbUrl, String user, String password, DataSourceFactory dsFactory) {
        Objects.requireNonNull(key, "La clave no puede ser nula");
        if(dsFactory == null) dsFactory = DEFAULT_DS_FACTORY;
        if(dsFactory == null) {
            throw new IllegalArgumentException("No se ha encontrado ningún DataSourceFactory en el classpath. Asegúrese de incluir una implementación, como sqlutils-hikaricp, en las dependencias del proyecto o defina la suya propia.");
        }

	    JdbcConnection instance = new JdbcConnection(key, dbUrl, user, password, dsFactory);

        if(instances.putIfAbsent(key, instance) != null) {
            logger.debug("Otro hilo creó una instancia para la clave {}: cerrando la recién creada", key);
            instance.close();
            throw new IllegalStateException("Ya hay una instancia asociada a la clave");
        }
        logger.debug("Creado nuevo pool de conexiones para la clave {}", key);

        return instance;
    }

    /**
     * Genera una instancia {@link JdbcConnection} sin proporcionar explícitamente una fábrica de DataSource.
     * <p> Esta versión del método utiliza la fábrica de DataSource por defecto, que se busca automáticamente
     * en el classpath mediante el mecanismo de ServiceLoader.
     * @param key Clave que identifica la conexión.
     * @param dbUrl La URL de conexión.
     * @param user El usuario de conexión (puede ser nulo si el DataSourceFactory lo permite).
     * @param password La contraseña de conexión (puede ser nula si el DataSource
     * @return La instancia solicitada.
     * @throws IllegalStateException Si la instancia ya existe.
     * @throws IllegalArgumentException Si no se detecta ninguna fábrica de DataSource disponible en el classpath.
     *    La librería sqlutils-hikaricp incluye una implementación de DataSourceFactory basada en HikariCP, así
     *    que puede cargarse como dependencia si no se plantea definir una implementación propia.
     */
    public static JdbcConnection create(String key, String dbUrl, String user, String password) {
        return JdbcConnection.create(key, dbUrl, user, password, null);
    }

    /**
     * Obtiene la instancia asociada a una clave.
     * @param key Clave que identifica la conexión.
     * @return La instancia solicitada.
     * @throws IllegalArgumentException Si no existe la instancia para la clave suministrada.
     */
    public static JdbcConnection get(String key) {
        JdbcConnection instance = instances.get(key);
        if(instance == null) throw new IllegalStateException("No existe una instancia para la clave %s".formatted(key));

        if(instance.isOpen()) {
            logger.debug("Hallado pool abierto de conexiones para la clave {}", key);
            return instance;
        }
        else {
            logger.debug("Hallado pool de conexiones para la clave {}, pero no está abierto", key);
            instances.remove(key, instance);
            throw new IllegalStateException("La instancia solicitada no existe.");
        }
    }

    /** Obtiene el DataSource asociado al pool de conexiones
     * @return El DataSource asociado
     */
    public DataSource getDataSource() {
        if(tm != null) logger.warn("Hay un gestor de transacciones asociado a este pool '{}'. A menos de que esté seguro de lo que hace, debería obtener las conexiones a través de él.", key);

        return ds;
    }

    @Override
    protected TransactionManager createTransactionManager() {
        return TransactionManager.create(key, ds);
    }

    /**
     * Cierra el {@link DataSource} asociado a esta conexión, siempre que éste implemente {@link AutoCloseable}.
     * Si no lo implementa, no hace nada.
     */
    @Override
    protected void closeResource() {
        if(ds instanceof AutoCloseable ac) {
            try {
                ac.close();
            } catch (Exception e) {
                logger.error("Error al cerrar el DataSource del pool de conexiones para la clave {}: {}", key, e.getMessage(), e);
            }
        }
    }

    @Override
    protected void removeInstance() {
        instances.remove(key, this);
    }
}