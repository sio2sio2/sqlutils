package edu.acceso.sqlutils.internal;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.internal.tx.BaseTransactionManager;
import edu.acceso.sqlutils.tx.event.EventListener;

/**
 * Interfaz que representa una conexión a una base de datos
 * @param <TM> El tipo del gestor de transacciones asociado a esta conexión.
 */
public abstract class BaseConnection<TM extends BaseTransactionManager<?>> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BaseConnection.class);

    /**
     * Clave que identifica a la conexión. Esta misma clave se utiliza para asociar un gestor de transacciones específico a esta conexión.
     */
    protected final String key;
    /**
     * Sirve para determinar si la conexión está cerrada. Se utiliza un {@link AtomicBoolean} para garantizar la seguridad en entornos concurrentes.
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /**
     * Gestor de transacciones asociado a esta conexión. Es volatile para garantizar la visibilidad de los cambios en entornos concurrentes.
     */
    protected volatile TM tm;
    

    /**
     * Constructor que asigna la clave a la conexión.
     * @param key La clave que identifica a la conexión. 
     */
    protected BaseConnection(String key) {
        this.key = key;
    }

    /**
     * Devuelve la clave que identifica a la conexión.
     * @return La clave solicitada.
     */
    public String getKey() {
        return key;
    }

    /**
     * Indica si la conexión está abierta
     * @return {@code true} si la conexión está abierta, {@code false} si está cerrada.
     */
    public boolean isOpen() {
        return !closed.get();
    }

    /**
     * Crea un gestor de transacciones asociado a este conector. Este método es abstracto y debe ser implementado por las clases que extiendan {@code BaseConnection} para proporcionar la lógica específica de creación del gestor de transacciones.
     * @return El gestor de transacciones creado.
     */
    protected abstract TM createTransactionManager();

    /**
     * Crea un gestor de transacciones asociado a este conector
     * @return El gestor de transacciones creado.
     */
    public BaseConnection<TM> withTransactionManager() {
        return withTransactionManager(null);
    }

    /**
     * Intenta crear un gestor de transacciones asociado a este conector
     * @param listeners Mapa con entradas clave-listener para añadir los listeners al gestor de transacciones.
     * Tenga presente que si el orden de ejecución de los listeners es importante, debe usar
     * un {@link java.util.LinkedHashMap} o similar para que se preserve el orden de inserción.
     * Si el mapa es {@code null}, no se añadirá ningún listener al gestor de transacciones.
     * @return El gestor de transacciones creado.
     */
    public synchronized BaseConnection<TM> withTransactionManager(Map<String, EventListener> listeners) {
        if(!isOpen()) throw new IllegalStateException("El objeto está cerrado");
        if(tm != null) throw new IllegalStateException("Ya hay un gestor de transacciones asociado a la clave '%s'. No se puede crear otro.".formatted(key)); 
        tm = createTransactionManager();
        if(listeners != null) listeners.forEach(tm::addEventListener);
        logger.debug("Creado un gestor de transacciones asociado a la clave '{}' de este", key);
        return this;
    }

    /**
     * Obtiene el gestor de transacciones asociado.
     * @return El gestor de transacciones solicitado
     */
    public synchronized TM getTransactionManager() {
        if(!isOpen()) throw new IllegalStateException("El conector está cerrado");
        if(tm == null) throw new IllegalStateException("No hay un gestor de transacciones asociado a este conector '%s'. Debe crearlo primero con initTransactionManager().".formatted(key));
        return tm;
    }

    /**
     * Cierra la conexión y el gestor de transacciones asociado, si existe.
     * Este método es idempotente, esto es, se puede llamar varias veces sin problemas
     * puesto que, si la conexión ya está cerrada, no hace nada.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (tm != null) {
                tm.close();
                tm = null;
            }
            closeResource();
            removeInstance();
        }
    }

    /**
     * Cierra el recurso asociado a la conexión (p. ej. DataSource, EntityManagerFactory, etc.).
     */
    protected abstract void closeResource();
    /**
     * Elimina la instancia del gestor de transacciones asociado.
     */
    protected abstract void removeInstance();
}