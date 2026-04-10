package edu.acceso.sqlutils.jdbc.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.internal.tx.BaseTransactionManager;
import edu.acceso.sqlutils.internal.tx.TransactionHandle;
import edu.acceso.sqlutils.jdbc.internal.tx.JdbcHandle;

/**
 * Particularización de {@link BaseTransactionManager} para JDBC, que gestiona transacciones utilizando
 * conexiones obtenidas de un {@link DataSource}.
 * Cada instancia de {@link TransactionManager} se identifica por una clave única, lo que permite
 * gestionar múltiples gestores de transacciones en la misma aplicación, cada uno con su propia fuente de datos.
 * <p>
 * Para crear una instancia de {@link TransactionManager}, se debe llamar al método estático
 * {@link #create(String, DataSource)}, proporcionando una clave única y un {@link DataSource} válido.
 * Para obtener una instancia ya creada, se puede usar el método {@link #get(String)} con la clave correspondiente:
 * <pre>
 * // Crear un gestor de transacciones JDBC
 * DataSource ds = ...; // Configurar el DataSource según las necesidades de la aplicación
 * TransactionManager tm = TransactionManager.create("miBD", ds);
 * </pre>
 * <p>
 * El {@link TransactionManager} se encarga de crear y gestionar los {@link TransactionHandle} de tipo {@link JdbcHandle},
 * que a su vez manejan las conexiones JDBC y las operaciones de transacción (begin, commit, rollback) de manera segura
 * y transparente para el programador.
 */
public class TransactionManager extends BaseTransactionManager<Connection> {
    private static Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    /** Instancias de gestores de transacciones accesibles por clave */
    private static final Map<String, TransactionManager> instances = new ConcurrentHashMap<>();

    /** La fuente de datos utilizada para obtener conexiones JDBC en esta instancia de TransactionManager */
    private final DataSource ds;

    /**
     * Constructor privado para crear una nueva instancia de {@link TransactionManager}.
     * @param key La clave única que identifica a esta instancia de gestor de transacciones.
     * @param ds El {@link DataSource} que se usará para obtener conexiones JDBC en esta instancia.
     */
    private TransactionManager(String key, DataSource ds) {
        super(key);
        this.ds = ds;
    }

    /**
     * Crea una nueva instancia de {@link TransactionManager} con la clave y el {@link DataSource} proporcionados.
     * @param key La clave única que identificará a esta instancia de gestor de transacciones. Debe ser diferente a las claves de otras instancias ya creadas.
     * @param ds El {@link DataSource} que se usará para obtener conexiones JDBC en esta instancia.
     * @return La instancia creada.
     * @throws IllegalStateException Si ya existe una instancia de {@link TransactionManager} registrada con la misma clave.
     * @throws NullPointerException Si el {@link DataSource} proporcionado es {@code null}.
     */
    public static TransactionManager create(String key, DataSource ds) {
        Objects.requireNonNull(ds, "Debe proporcionarse un DataSource para crear un gestor de transacciones JDBC");
        TransactionManager newInstance = new TransactionManager(key, ds);

        if(instances.putIfAbsent(key, newInstance) != null) {
            throw new IllegalStateException("Ya existe un gestor de transacciones registrado con la clave '%s'".formatted(key));
        }
        logger.debug("Creado el gestor de transacciones JDBC llamado '{}'", key);

        return newInstance;
    }

    /**
     * Crea una nueva instancia de {@link TransactionManager} que no se registra en el mapa de instancias.
     * Este método es interno y no debería usarse directamente. Está pensado para que la clase
     * {@link edu.acceso.sqlutils.jdbc.JdbcConnection} gestione su propio {@link TransactionManager},
     * de modo que sólo sea accesible a través de ella.
     * @param ds El {@link DataSource} que se usará para obtener conexiones JDBC en esta instancia.
     * @return La instancia creada.
     * @throws NullPointerException Si el {@link DataSource} proporcionado es {@code null}.
     */
    public static TransactionManager createInternal(DataSource ds) {
        Objects.requireNonNull(ds, "Debe proporcionarse un DataSource para crear un gestor de transacciones JDBC");
        String key = "internal-%d".formatted(System.identityHashCode(ds));
        return new TransactionManager(key, ds);
    }

    /**
     * Obtiene la instancia de {@link TransactionManager} registrada con la clave proporcionada.
     * @param key La clave única que identifica al gestor de transacciones que se desea obtener.
     * @return La instancia de {@link TransactionManager} asociada a la clave proporcionada.
     * @throws IllegalStateException Si no existe una instancia de {@link TransactionManager} registrada con la clave proporcionada.
     *    En este caso, se recomienda llamar a {@link #create(String, DataSource)} primero para crear la instancia antes de intentar obtenerla.
     */
    public static TransactionManager get(String key) {
        TransactionManager instance = instances.get(key);
        if(instance == null) throw new IllegalStateException("El gestor de transacciones '%s' no ha sido inicializado. Llame a create() primero.".formatted(key));
        return instance;
    }

    /**
     * Elimina esta instancia de {@link TransactionManager} del registro de instancias, permitiendo que sea recolectada por el GC.
     * Este método se llama automáticamente cuando se cierra el gestor de transacciones, para liberar
     */
    @Override
    protected void removeInstance() {
        instances.remove(getKey(), this);
        logger.debug("Eliminado el gestor de transacciones JDBC llamado '{}'", getKey());
    }

    /**
     * Crea un nuevo {@link JdbcHandle} utilizando una conexión obtenida del {@link DataSource} asociado a esta instancia de {@link TransactionManager}.
     * @return Un nuevo {@link JdbcHandle} listo para gestionar una transacción JDBC.
     * @throws DataAccessException Si ocurre un error al intentar obtener una conexión del {@link DataSource} o al crear el {@link JdbcHandle}.
     */
    @Override
    protected TransactionHandle<Connection> createHandle() throws DataAccessException {
        try {
            return new JdbcHandle(ds.getConnection());
        } catch(SQLException e) {
           throw new DataAccessException("No puede obtenerse una conexión a la base de datos", e); 
        }
    }
}