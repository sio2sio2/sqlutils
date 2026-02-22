package edu.acceso.sqlutils.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Gestor multihilo para manejar transacciones de base de datos.
 * <p>Cuando se ha abierto una transacción, proporciona una conexión por hilo.
 * Permite anidar transacciones y soporta multiples bases de datos mediante una clave identificativa.
 * Implementa el patrón Multiton (varias instancias singleton diferenciadas por una clave).
 * <p>Para crear una instancia, use el método estático {@link #create(String, DataSource)}:
 * <pre>
 *    TransactionManager tm = TransactionManager.create("gestor1", dataSource);
 * </pre>
 * <p>Una vez creada la instancia, deben utilizarse sus métodos {@link #transaction(TransactionableR)}
 * o {@link #transaction(Transactionable)} para ejecutar operaciones dentro de una transacción:
 * <pre>
 *    tm.transaction(conn -&gt; {
 *       // operaciones con la conexión dentro de una transacción
 *       // La conexión está protegida contra el cierre accidental
 *    });
 * </pre>
 * <p>Si se decide manejar manualmente la creación y cierrre de transacciones, puede consultarse el código
 * del método {@link #transaction(TransactionableR)} para ver cómo hacerlo correctamente.
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    /** Instancias de gestores de transacciones accesibles por clave */
    private static final Map<String, TransactionManager> instances = new ConcurrentHashMap<>();

    /** Conexión por hilo para manejar transacciones anidadas */
    private final ThreadLocal<Transaction> contextHolder;

    /** Lista de listeners de eventos para todas las transacciones */
    private final Set<EventListener> listeners = new CopyOnWriteArraySet<>();
    /** Mapa para almacenar claves asociadas a los listeners persistentes y así poderlos recuperar luego */
    private final Map<String, EventListener> listenerKeys = new ConcurrentHashMap<>();

    /** Mapa para almacenar claves asociadas a los listeners efímeros */
    private final ThreadLocal<Map<String, EventListener>> ephemeralListeners = ThreadLocal.withInitial(LinkedHashMap::new);

    /**
     * El DataSource asociado a cada gestor de transacciones.
     */
    private final DataSource ds;
    /**
     * La clave asociada a la instancia.
     */
    private final String key;

    /**
     * Interfaz funcional para operaciones de transacción que devuelven un valor.
     * @param <T> Tipo del valor devuelto.
     */
    @FunctionalInterface
    public static interface TransactionableR<T> {
        /**
         * Ejecuta las operaciones de la transacción.
         * @param ctxt El contexto de la transacción.
         * @return El valor devuelto por las operaciones.
         * @throws Throwable Si ocurre un error durante las operaciones.
         */
        T run(TransactionContext ctxt) throws Throwable;
    }

    /**
     * Interfaz funcional para operaciones de transacción sin valor devuelto.
     */
    @FunctionalInterface
    public static interface Transactionable {
        /**
         * Ejecuta las operaciones de la transacción.
         * @param ctxt El contexto de la transacción.
         * @throws Throwable Si ocurre un error durante las operaciones.
         */
        void run(TransactionContext ctxt) throws Throwable;
    }

    /** Constructor protegido para implementar el patrón Multiton */
    protected TransactionManager(String key, DataSource ds) {
        this.key = key;
        this.ds = ds;

        if(instances.putIfAbsent(key, this) != null) throw new IllegalStateException("La conexión '%s' ya tenía creada un gestor de transacciones".formatted(key));
        else logger.debug("Creado gestor de transacciones para la conexión '{}'", key);

        this.contextHolder = new ThreadLocal<>();
    }

    /**
     * Crea un gestor de transacciones asociado a una clave y un DataSource.
     * @param key Clave identificativa del gestor.
     * @param ds DataSource asociado al gestor.
     * @return El gestor de transacciones creado.
     * @throws IllegalStateException Si el gestor ya ha sido inicializado.
     */
    public static TransactionManager create(String key, DataSource ds) {
        return new TransactionManager(key, ds);
    }

    /**
     * Obtiene el gestor de transacciones asociado a una clave.
     * @param key Clave identificativa.
     * @return El gestor de transacciones asociado a la clave.
     */
    public static TransactionManager get(String key) {
        TransactionManager tm = instances.get(key);
        if(tm == null) throw new IllegalStateException("El gestor de transacciones '%s' no ha sido inicializado. Llame a create() primero.".formatted(key));
        return tm;
    }

    /**
     * Obtiene la conexión asociada a la transacción actual.
     * @return La conexión asociada a la transacción actual.
     */
    public Connection getConnection() {
        return contextHolder.get().getContext().connection();
    }

    /**
     * Obtiene el contexto de la transacción actual.
     * @return El contexto solicitado.
     * @throws IllegalStateException Si no hay una transacción activa en el hilo actual.
     */
    public TransactionContext getContext() {
        if(!isActive()) throw new IllegalStateException("No hay ninguna transacción activa en el hilo '%s'".formatted(Thread.currentThread().getName()));
        return contextHolder.get().getContext();
    }

    /**
     * Ejecuta operaciones dentro de una transacción.
     * @param <T> Tipo del valor devuelto.
     * @param operations Operaciones a ejecutar dentro de la transacción.
     * @return El valor devuelto por las operaciones.
     * @throws DataAccessException Si ocurre un error durante las operaciones
     */
    public <T> T transaction(TransactionableR<T> operations) throws DataAccessException {
        T value = null;

        Transaction context = contextHolder.get();
        boolean isNewTransaction = !isActive();

        if(isNewTransaction) {
            try {
                Map<String, EventListener> txListeners = new LinkedHashMap<>();
                for(EventListener listener: listeners) {
                    String key = listenerKeys.entrySet().stream()
                        .filter(e -> e.getValue() == listener)
                        .map(Map.Entry::getKey)
                        .findFirst().orElseThrow(() -> new IllegalStateException("Problema de concurrencia: El listener '%s' no tiene clave asociada".formatted(listener)));

                    txListeners.put(key, listener);
                }
                
                txListeners.putAll(ephemeralListeners.get());    
                ephemeralListeners.get().clear();

                context = new Transaction(getKey(), ds.getConnection(), txListeners);
                contextHolder.set(context);
            } catch (SQLException e) {
                throw new DataAccessException("Error al obtener la conexión del DataSource", e);
            }
        }

        try {
            context.begin();
            value = operations.run(this.getContext());
            context.commit();
        } catch (Throwable e) {
            context.rollback(e);
        }
        finally {
            if(isNewTransaction) contextHolder.remove();
        }

        return value;
    }

    /**
     * Ejecuta operaciones dentro de una transacción.
     * @param operations Operaciones a ejecutar dentro de la transacción.
     * @throws DataAccessException Si ocurre un error durante las operaciones
     */
    public void transaction(Transactionable operations) throws DataAccessException {
        transaction(conn -> {
            operations.run(conn);
            return null;
        });
    }

    /**
     * Indica si en el hilo actual la transacción está abierta.
     * @return {@code true}, si la transacción está abierta.
     */
    public boolean isActive() {
        return contextHolder.get() != null;
    }

    /**
     * Devuelve la clave asociada a la instancia.
     * @return La clave asociada a la instancia.
     */
    public String getKey() {
        return key;
    }

    /**
     * Obtiene los nombres de los gestores de transacciones disponibles.
     * @return Los nombres de los gestores de transacciones.
     */
    public static String[] getKeys() {
        return instances.keySet().toArray(new String[0]);
    }

    /**
     * Añade un listener persistente para su ejecución en todas las transacciones del gestor.
     * @param key La clave identificativa del listener.
     * @param listener El listener que se desea añadir.
     * @throws IllegalStateException Si ya existe un listener registrado con la clave dada.
     */
    public void addListener(String key, EventListener listener) {
        if(listenerKeys.putIfAbsent(key, listener) != null) throw new IllegalStateException("Ya hay un listener registrado con la clave '%s'".formatted(key));
        listeners.add(listener);

        // Si el listener es context-aware, le proporcionamos un mecanismo para que pueda acceder
        // al contexto de la transacción actual en cualquier momento.
        if(listener instanceof ContextAwareEventListener contextAware) {
            Supplier<EventListenerContext> supplier = () -> {
                if(!isActive()) throw new IllegalStateException("No hay ninguna transacción activa en el hilo '%s'".formatted(Thread.currentThread().getName()));
                Transaction tx = contextHolder.get();
                return tx.getEventListenerContext(listener);
            };

            contextAware.setContextSupplier(supplier);
        }
    }

    /**
     * Elimina un listener persistente registrado con la clave dada.
     * @param key La clave identificativa del listener a eliminar.
     */
    public void removeListener(String key) {
        EventListener listener = listenerKeys.remove(key);
        if(listener != null) listeners.remove(listener);
    }

    /**
     * Obtiene un listener persistente registrado con la clave dada.
     * @param key La clave que identifica al listener deseado.
     * @return El listener registrado con la clave dada, o {@code null} si no existe.
     */
    public EventListener getListener(String key) {
        return listenerKeys.get(key);
    }

    /**
     * Obtiene un listener persistente registrado con la clave dada y lo castea al tipo especificado.
     * @param <T> Tipo del listener esperado.
     * @param key La clave que identifica al listener deseado.
     * @param type La clase del tipo del listener esperado.
     * @return El listener registrado con la clave dada, o {@code null} si no existe.
     * @throws IllegalStateException Si el listener registrado con la clave dada no es del tipo especificado.
     */
    public <T extends EventListener> T getListener(String key, Class<T> type) {
        EventListener listener = getListener(key);
        if(listener == null) return null;
        if(!type.isInstance(listener)) throw new IllegalStateException("El listener registrado con la clave '%s' no es del tipo '%s'".formatted(key, type.getName()));
        return type.cast(listener);
    }

    /**
     * Añade un listener efímero para su ejecución sólo en la próxima transacción del hilo.
     * @param key La clave identificativa del listener.
     * @param listener El listener que se desea añadir.
     */
    public void addEphemeralListener(String key, EventListener listener) {
        if(ephemeralListeners.get().containsKey(key)) throw new IllegalStateException("Ya hay un listener efímero registrado con la clave '%s'".formatted(key));
        ephemeralListeners.get().put(key, listener);
    }
}