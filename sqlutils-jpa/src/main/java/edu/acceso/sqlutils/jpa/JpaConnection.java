package edu.acceso.sqlutils.jpa;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.internal.BaseConnection;
import edu.acceso.sqlutils.jpa.tx.TransactionManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;

/**
 * Implementación de {@link BaseConnection} para JPA. Proporciona una conexión a una unidad de persistencia
 * y un gestor de transacciones asociado. Las instancias se gestionan a través de un mapa estático,
 * lo que permite compartir la misma conexión entre diferentes partes de la aplicación (patrón Multiton).
 * <p>
 * Para crear una instancia, se debe utilizar el método estático {@link #create(String, Map)}, que
 * recibe el nombre de la unidad de persistencia y un mapa de propiedades opcional.
 * Si ya existe una instancia para la unidad de persistencia especificada, se lanzará una excepción:
 * <pre>
 * try {
 *     JpaConnection conn1 = JpaConnection.create("myPersistenceUnit", props);
 *     JpaConnection conn2 = JpaConnection.create("myPersistenceUnit", props); // Esto lanzará una IllegalStateException
 * } catch (IllegalStateException e) {
 *     System.out.println("No se puede crear una segunda instancia para la misma unidad de persistencia: " + e.getMessage());
 * }
 * </pre>
 * <p>
 * Para obtener una instancia existente, se puede utilizar el método estático {@link #get(String)},
 * que recibe el nombre de la unidad de persistencia.
 */
public class JpaConnection extends BaseConnection<TransactionManager> {
    private static final Logger logger = LoggerFactory.getLogger(JpaConnection.class);

    /** Mapa con las instancias creadas **/
    private static final Map<String, JpaConnection> instances = new ConcurrentHashMap<>();

    /** El gestor de transacciones asociado a esta instancia. */
    private final EntityManagerFactory emf;

    /**
     * Constructor privado para evitar la creación directa de instancias. Se debe utilizar el método estático {@link #create(String, Map)}.
     * @param persistenceUnit El nombre de la unidad de persistencia.
     * @param props El mapa que define las propiedades definidas en tiempo de ejecución.
     */
    private JpaConnection(String persistenceUnit, Map<String, String> props) {
        super(persistenceUnit);
        this.emf = Persistence.createEntityManagerFactory(persistenceUnit, props);
    }

    /**
     * Crea una instancia a partir de los datos de conexión.
     * @param persistenceUnit El nombre de la unidad de persistencia.
     * @param props El mapa que define las propiedades definidas en tiempo de ejecución.
     * @return La instancia solicitada.
     * @throws IllegalStateException Si la instancia ya existe.
     * @throws IllegalArgumentException Si no hay DataSourceFactory disponible ni se ha proporcionado como argumento.
     */
    public static JpaConnection create(String persistenceUnit, Map<String, String> props) {
        Objects.requireNonNull(persistenceUnit, "El nombre de la unidad de persistencia no puede ser nulo");
        if(props == null) props = Collections.emptyMap();

        JpaConnection instance = new JpaConnection(persistenceUnit, props);

        if(instances.putIfAbsent(persistenceUnit, instance) != null) {
            logger.debug("Otro hilo creó una instancia para la unidad de persistencia {}: cerrando la recién creada", persistenceUnit);
            instance.close();
            throw new IllegalStateException("Ya hay una instancia asociada a la unidad de persistencia");
        }
        logger.debug("Instancia de JpaConnection creada para la unidad de persistencia '{}'", persistenceUnit);

        return instance;
    }

    /**
     * Crea una instancia a partir de los datos de conexión.
     * @param persistenceUnit El nombre de la unidad de persistencia.
     * @return La instancia solicitada.
     * @throws IllegalStateException Si la instancia ya existe.
     * @throws IllegalArgumentException Si no hay DataSourceFactory disponible ni se ha proporcionado como argumento.
     */
    public static JpaConnection create(String persistenceUnit) {
        return create(persistenceUnit, null);
    }

    /**
     * Indica si la fábrica de gestores de entidad asociada a la entidad está abierta
     * @return true si está abierta, false en caso contrario.
     */
    @Override
    public boolean isOpen() {
        return super.isOpen() && emf.isOpen();
    }

    /**
     * Devuelve un objeto EntityManagerFactory generado anteriormente.
     * @param persistenceUnit El nombre de la unidad de persistencia.
     * @return La instancia de {@link JpaConnection} correspondiente.
     * @throws IllegalArgumentException Si el índice está fuera de rango.
     */
    public static JpaConnection get(String persistenceUnit) {
        Objects.requireNonNull(persistenceUnit, "El nombre de la unidad de persistencia no puede ser nulo");

        JpaConnection instance = instances.get(persistenceUnit);
        if(instance == null) throw new IllegalArgumentException("No existe ningún objeto asociado a ese nombre de unidad de persistencia");

        if(instance.isOpen()) return instance;
        else {
            instances.remove(persistenceUnit, instance);
            throw new IllegalStateException("La instancia solicitada no existe.");
        }
    }

    /** Obtiene el DataSource asociado al pool de conexiones
     * @return El DataSource asociado
     */
    public EntityManagerFactory getEntityManagerFactory() {
        if(tm != null) logger.warn("Hay un gestor de transacciones asociado a este pool '{}'. A menos de que esté seguro de lo que hace, debería obtener las conexiones a través de él.", key);

        return emf;
    }

    /**
     * Crea un gestor de transacciones asociado a esta conexión.
     */
    @Override
    protected TransactionManager createTransactionManager() {
        return TransactionManager.create(key, emf);
    }

    /**
     * Cierra la fábrica de gestores de entidad asociada a esta conexión.
     */
    @Override
    protected void closeResource() {
        try {
            emf.close();
        } catch (IllegalStateException e) {
            logger.error("El EntityManagerFactory para la unidad de persistencia '{}' ya había sido cerrado.", key, e);
        }
    }

    @Override
    protected void removeInstance() {
        instances.remove(key, this);
    }
}
