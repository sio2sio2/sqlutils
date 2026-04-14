package edu.acceso.sqlutils.internal.tx;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.tx.TransactionContext;
import edu.acceso.sqlutils.tx.Transactionable;
import edu.acceso.sqlutils.tx.TransactionableR;
import edu.acceso.sqlutils.tx.event.ContextAwareEventListener;
import edu.acceso.sqlutils.tx.event.EventListener;
import edu.acceso.sqlutils.tx.event.EventListenerContext;
import edu.acceso.sqlutils.tx.event.LoggingManager;

/**
 * Gestor multihilo para manejar transacciones de base de datos.
 * @param <R> El tipo del recurso que maneja la transacción (p. ej. {@link java.sql.Connection} si usamos bases de datos con JDBC).
 * Es una clase genérica que permite el manejo de transacciones JDBC o JPA,
 * según se base en objetos {@link javax.sql.DataSource} o {@link jakarta.persistence.EntityManagerFactory}.
 * <p>Cuando se ha abierto una transacción, proporciona una conexión por hilo.
 * Permite anidar transacciones y soporta multiples bases de datos mediante una clave identificativa.
 * Implementa el patrón Multiton (varias instancias singleton diferenciadas por una clave).
 * <p>Permite ejecutar transacciones del siguiente modo:
 * <pre>
 *    tm.transaction(ctxt -&gt; {
 *       // ctxt es un objeto de contexto que proporciona acceso
 *       // a la conexión de la transacción actual mediante ctxt.handle().
 *       // En caso de JDBC esta conexión sería un objeto Connection, y en caso de JPA sería un EntityManager.
 *    });
 * </pre>
 */
public abstract class BaseTransactionManager<R> {
    private static final Logger logger = LoggerFactory.getLogger(BaseTransactionManager.class);

    /** Contador para transacciones activas */
    private final AtomicInteger activeTransactions = new AtomicInteger(0);

    /** Conexión por hilo para manejar transacciones anidadas */
    private final ThreadLocal<TransactionExecution<R>> contextHolder;

    /** 
     * Mapa para almacenar claves asociadas a los listeners persistentes y así poderlos recuperar
     * en cualquier momento. La clave puede ser arbitriaria, pero se recomienda usar una constante
     * estática en la clase del listener para evitar colisiones. Véase {@link LoggingManager#KEY} como ejemplo.
     */
    private final Map<String, EventListener> listeners = Collections.synchronizedMap(new LinkedHashMap<>());

    /**
     * Mapa para almacenar claves asociadas a los listeners efímeros.
     * La clave puede ser arbitriaria, pero se recomienda usar una constante estática en la clase del listener.
     * 
     * <p>Estos listeners sólo son válidos para la próxima transacción del propio hilo, así que
     * no hay problemas de concurrencia; y basta con usar un único mapa ordenado normal ({@link LinkedHashMap}),
     * a diferencia de los listeners persistentes que sí tiene tienen problemas con la
     * concurrencia y requieren dos estructuras de datos.
     */
    private final ThreadLocal<Map<String, EventListener>> ephemeralListeners = ThreadLocal.withInitial(LinkedHashMap::new);

    /** Clave para identificar la instancia del gestor de transacciones */
    private final String key;

    /**
     * Constructor protegido para evitar la creación directa de instancias fuera de las subclases.
     * @param key La clave identificativa de la instancia del gestor de transacciones.
     */
    protected BaseTransactionManager(String key) {
        this.key = key;
        this.contextHolder = new ThreadLocal<>();
    }

    /**
     * Devuelve el recurso asociado a la transacción activa, que puede ser un {@link java.sql.Connection}, un {@link jakarta.persistence.EntityManager}, etc.
     * @return El recurso solicitado.
     * @throws IllegalStateException Si no hay una transacción activa en el hilo actual. 
     */
    public R getHandle() {
        return getContext().handle();
    }

    /**
     * Obtiene el contexto de la transacción actual.
     * @return El contexto solicitado.
     * @throws IllegalStateException Si no hay una transacción activa en el hilo actual.
     */
    public TransactionContext<R> getContext() {
        if(!isActive()) throw new IllegalStateException("No hay ninguna transacción activa en el hilo '%s'".formatted(Thread.currentThread().getName()));
        return contextHolder.get().getContext();
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
     * Cierra el gestor de transacciones, liberando los recursos asociados.
     * @throws IllegalStateException Si hay transacciones activas en el momento del cierre.
     */
    protected abstract void removeInstance();

    /**
     * Cierra el gestor de transacciones, liberando los recursos asociados.
     * @throws IllegalStateException Si hay transacciones activas en el momento del cierre.
     */
    public void close() {
        if(activeTransactions.get() > 0) {
            throw new IllegalStateException("No se puede cerrar el gestor de transacciones '%s' porque hay %d transacciones activas".formatted(getKey(), activeTransactions.get()));
        }
        removeInstance();
        logger.debug("Gestor de transacciones asociado a la clave '{}' cerrado", key);
    }

    /**
     * Crea un nuevo handle de transacción (Connection, EntityManager, etc.) para la transacción actual.
     * @return El handle creado.
     * @throws DataAccessException Si ocurre un error al crear el handle de transacción.
     */
    protected abstract TransactionHandle<R> createHandle() throws DataAccessException;

    /**
     * Ejecuta operaciones dentro de una transacción.
     * @param <T> Tipo del valor devuelto.
     * @param operations Operaciones a ejecutar dentro de la transacción.
     * @return El valor devuelto por las operaciones.
     * @throws DataAccessException Si ocurre un error durante las operaciones
     */
    public <T> T transaction(TransactionableR<R, T> operations) throws DataAccessException {
        T value = null;

        TransactionExecution<R> context = contextHolder.get();
        boolean isNewTransaction = !isActive();

        if(isNewTransaction) {
            Map<String, EventListener> txListeners = new LinkedHashMap<>(listeners);
            txListeners.putAll(ephemeralListeners.get());    
            ephemeralListeners.get().clear();

            context = new TransactionExecution<>(getKey(), createHandle(), txListeners);
            contextHolder.set(context);
            activeTransactions.incrementAndGet();
        }

        try {
            context.begin();
            value = operations.run(this.getContext());
            context.commit();
        } catch (Throwable e) {
            context.rollback(e);
        } finally {
            if(isNewTransaction) {
                contextHolder.remove();
                activeTransactions.decrementAndGet();
            }
        }

        return value;
    }

    /**
     * Ejecuta operaciones dentro de una transacción.
     * @param operations Operaciones a ejecutar dentro de la transacción.
     * @throws DataAccessException Si ocurre un error durante las operaciones
     */
    public void transaction(Transactionable<R> operations) throws DataAccessException {
        transaction(ctxt -> {
            operations.run(ctxt);
            return null;
        });
    }
    
    /**
     * Añade un listener persistente para su ejecución en todas las transacciones del gestor.
     * @param key La clave identificativa del listener.
     * @param listener El listener que se desea añadir.
     * @throws IllegalStateException Si ya existe un listener registrado con la clave dada.
     * @return El propio gestor de transacciones para permitir encadenar llamadas al método.
     */
    public BaseTransactionManager<R> addEventListener(String key, EventListener listener) {
        if(listeners.putIfAbsent(key, listener) != null) throw new IllegalStateException("Ya hay un listener registrado con la clave '%s'".formatted(key));

        // Si el listener es context-aware, le proporcionamos un mecanismo para que pueda acceder
        // al contexto de la transacción actual en cualquier momento.
        if(listener instanceof ContextAwareEventListener contextAware) {
            Supplier<EventListenerContext> supplier = () -> {
                if(!isActive()) throw new IllegalStateException("No hay ninguna transacción activa en el hilo '%s'".formatted(Thread.currentThread().getName()));
                TransactionExecution<R> tx = contextHolder.get();
                return tx.getEventListenerContext(listener);
            };

            contextAware.setContextSupplier(supplier);
        }
        return this;
    }

    /**
     * Elimina un listener persistente registrado con la clave dada.
     * @param key La clave identificativa del listener a eliminar.
     */
    public void removeEventListener(String key) {
        listeners.remove(key);
    }

    /**
     * Obtiene un listener persistente registrado con la clave dada.
     * @param key La clave que identifica al listener deseado.
     * @return El listener registrado con la clave dada, o {@code null} si no existe.
     */
    public EventListener getListener(String key) {
        return listeners.get(key);
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