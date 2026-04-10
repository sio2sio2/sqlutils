package edu.acceso.sqlutils.jpa.tx;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.internal.tx.BaseTransactionManager;
import edu.acceso.sqlutils.internal.tx.TransactionHandle;
import edu.acceso.sqlutils.jpa.internal.tx.JpaHandle;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceException;

/**
 * Particularización de {@link BaseTransactionManager} para JPA, que gestiona transacciones utilizando
 * EntityManagers obtenidos de un {@link EntityManagerFactory}.
 * Cada instancia de {@link TransactionManager} se identifica por una clave única, lo que permite
 * gestionar múltiples gestores de transacciones en la misma aplicación, cada uno con su propio EntityManagerFactory.
 * <p>
 * Para crear una instancia de {@link TransactionManager}, se debe llamar al método estático
 * {@link #create(String, EntityManagerFactory)}, proporcionando una clave única y un {@link EntityManagerFactory} válido.
 * Para obtener una instancia ya creada, se puede usar el método {@link #get(String)} con la clave correspondiente:
 * <pre>
 * // Crear un gestor de transacciones JPA
 * EntityManagerFactory emf = ...; // Configurar el EntityManagerFactory según las necesidades de la aplicación
 * TransactionManager tm = TransactionManager.create("miBD", emf);
 * </pre>
 * <p>
 * El {@link TransactionManager} se encarga de crear y gestionar los {@link TransactionHandle} de tipo {@link JpaHandle},
 * que a su vez manejan los EntityManagers y las operaciones de transacción (begin, commit, rollback) de manera segura
 * y transparente para el programador.
 */
public class TransactionManager extends BaseTransactionManager<EntityManager> {
    private static Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    /** Instancias de gestores de transacciones accesibles por clave */
    private static final Map<String, TransactionManager> instances = new ConcurrentHashMap<>();

    /** El EntityManagerFactory utilizado para obtener EntityManagers en esta instancia de TransactionManager */
    private final EntityManagerFactory emf;

    /**
     * Constructor privado para crear una nueva instancia de {@link TransactionManager}.
     * @param key La clave única que identifica a esta instancia de gestor de transacciones.
     * @param emf El {@link EntityManagerFactory} que se usará para obtener EntityManagers en esta instancia.
     */
    private TransactionManager(String key, EntityManagerFactory emf) {
        super(key);
        this.emf = emf;
    }

    /**
     * Crea una nueva instancia de {@link TransactionManager} con la clave y el {@link EntityManagerFactory} proporcionados.
     * @param key La clave única que identifica a esta instancia de gestor de transacciones.
     * @param emf El {@link EntityManagerFactory} que se usará para obtener EntityManagers en esta instancia.
     * @return La instancia de {@link TransactionManager} creada.
     * @throws IllegalStateException Si ya existe una instancia de {@link TransactionManager} registrada con la misma clave.
     * @throws NullPointerException Si el {@link EntityManagerFactory} proporcionado es null.
     */
    public static TransactionManager create(String key, EntityManagerFactory emf) {
        Objects.requireNonNull(emf, "Debe proporcionarse un EntityManagerFactory para crear un gestor de transacciones JPA");
        TransactionManager newInstance = new TransactionManager(key, emf);

        if(instances.putIfAbsent(key, newInstance) != null) {
            throw new IllegalStateException("Ya existe un gestor de transacciones registrado con la clave '%s'".formatted(key));
        }
        logger.debug("Creado el gestor de transacciones JPA llamado '{}'", key);

        return newInstance;
    }

    /**
     * Crea una nueva instancia de {@link TransactionManager} que no se registra en el mapa de instancias.
     * Este método es interno y no debería usarse directamente. Está pensado para que la clase
     * {@link edu.acceso.sqlutils.jpa.JpaConnection} gestione su propio {@link TransactionManager},
     * de modo que sólo sea accesible a través de ella.
     * @param emf El {@link EntityManagerFactory} que se usará para obtener EntityManagers en esta instancia.
     * @return La nueva instancia creada.
     * @throws NullPointerException Si el {@link EntityManagerFactory} proporcionado es null.
     */
    public static TransactionManager createInternal(EntityManagerFactory emf) {
        Objects.requireNonNull(emf, "Debe proporcionarse un EntityManagerFactory para crear un gestor de transacciones JPA");
        String key = "internal-%d".formatted(System.identityHashCode(emf));
        return new TransactionManager(key, emf);
    }

     /**
     * Devuelve la instancia de {@link TransactionManager} registrada con la clave proporcionada.
     * @param key La clave única que identifica al gestor de transacciones deseado.
     * @return La instancia de {@link TransactionManager} registrada con la clave proporcionada.
     * @throws IllegalStateException Si no existe una instancia de {@link TransactionManager} registrada con la clave proporcionada.
     */

    /**
     * Devuelve la instancia de {@link TransactionManager} registrada con la clave proporcionada.
     * @param key La clave única que identifica al gestor de transacciones deseado.
     * @return La instancia de {@link TransactionManager} registrada con la clave proporcionada.
     * @throws IllegalStateException Si no existe una instancia de {@link TransactionManager} registrada con la clave proporcionada.
     */
    public static TransactionManager get(String key) {
        TransactionManager instance = instances.get(key);
        if(instance == null) throw new IllegalStateException("El gestor de transacciones '%s' no ha sido inicializado. Llame a create() primero.".formatted(key));
        return instance;
    }

    /**
     * Elimina la instancia de {@link TransactionManager} registrada con la clave de esta instancia.
     * Este método se llama automáticamente cuando se cierra el gestor de transacciones, para liberar
     */
    @Override
    protected void removeInstance() {
        instances.remove(getKey(), this);
        logger.debug("Eliminado el gestor de transacciones JPA llamado '{}'", getKey());
    }

    /**
     * Crea un nuevo {@link JpaHandle} con un EntityManager obtenido del {@link EntityManagerFactory} de esta instancia.
     * @return Un nuevo {@link JpaHandle} con un EntityManager listo para usar en las operaciones de la transacción.
     * @throws DataAccessException Si ocurre un error al intentar obtener un EntityManager del {@link EntityManagerFactory} o al crear el {@link JpaHandle}.
     */
    @Override
    protected TransactionHandle<EntityManager> createHandle() throws DataAccessException {
        try {
            return new JpaHandle(emf.createEntityManager());
        } catch(PersistenceException e) {
           throw new DataAccessException("No puede obtenerse un EntityManager para la base de datos", e); 
        }
    }

}
