package edu.acceso.sqlutils.internal.tx.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.internal.tx.TransactionHandle;
import edu.acceso.sqlutils.tx.TransactionContext;
import edu.acceso.sqlutils.tx.event.ContextAwareEventListener;
import edu.acceso.sqlutils.tx.event.EventListener;
import edu.acceso.sqlutils.tx.event.EventListenerContext;

/**
 * Clase que modela la ejecución de una transacción y proporciona el contexto necesario para realizar
 * operaciones dentro de ella. Además permite ejecutar acciones específicas en los distintos
 * eventos del ciclo (inicio, commit, rollback) a través de los EventListeners asociados a la transacción.
 * @param <R> El tipo de recurso que maneja el {@link TransactionHandle} asociado a esta ejecución como un objeto {@link java.sql.Connection}.
 */
public class TransactionExecution<R> {
    private static Logger logger = LoggerFactory.getLogger(TransactionExecution.class);

    private final TransactionHandle<R> handle;
    private int level_a;
    private final Map<String, EventListener> listeners;
    private final String tmKey;

    private final Map<EventListener, Object> resources_a = new HashMap<>();

    /**
     * Crea una nueva ejecución de transacción con el handle y los listeners especificados.
     * @param key Clave que identifica al gestor que maneja esta transacción.
     * @param handle El handle que proporciona el recurso para esta transacción.
     * @param listeners Los listeners que se asociarán a esta transacción para responder a los eventos de su ciclo de vida.
     */
    public TransactionExecution(String key, TransactionHandle<R> handle, Map<String, EventListener> listeners) {
        Objects.requireNonNull(handle, "La transacción debe tener un handle asociado");
        this.handle = handle;
        this.listeners = listeners;
        this.tmKey = key;
        this.level_a = 0;
    }

    /**
     * Devuelve el nivel de anidamiento actual de esta ejecución de transacción.
     * @return El nivel de anidamiento solicitado.
     */
    int level() {
        return level_a;
    }

    /**
     * Devuelve el contexto para el listener especificado.
     * @param listener El listener para el que se desea obtener el contexto.
     * @return El contexto solicitado.
     */
    public EventListenerContext getEventListenerContext(EventListener listener) {
        return new EventListenerContext() {
            @Override
            public int level() {
                return level_a;
            }

            @Override
            public String key() {
                return tmKey;
            }

            @Override
            public <T> void setResource(T resource) {
                resources_a.put(listener, resource);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> T getResource() {
                return (T) resources_a.get(listener);
            }
        };
    }

    /**
     * Devuelve el contexto de esta ejecución de transacción.
     * @return El contexto solicitado.
     */
    public TransactionContext<R> getContext() {
        return new TransactionContext<R>() {
            @Override
            public int level() {
                return level_a;
            }

            @Override
            public String key() {
                return tmKey;
            }

            @Override
            public R handle() {
                return handle.getHandle();
            }

            @Override
            public EventListener getEventListener(String key) {
               return listeners.get(key);
            }

            @Override
            public <T extends EventListener> T getEventListener(String key, Class<T> type) {
                EventListener listener = listeners.get(key);
                if(listener == null) return null;
                if(!type.isInstance(listener)) {
                    throw new IllegalStateException("El listener asociado a la clave '%s' no es del tipo esperado ".formatted(type.getName()));
                }
                return type.cast(listener);
            }
        };
    }   

    /**
     * Comienza la ejecución de la transacción. Si ya se ha comenzado, incrementa el nivel de anidamiento y lo notifica a los listeners.
     * @throws DataAccessException Si ocurre un error al arrancar la transacción.
     */
    public void begin() throws DataAccessException {
        // La transacción sólo empieza realmente en el primer nivel de anidamiento.
        if(level_a > 0) {
            level_a++;
            logger.trace("Transacción anidada asociada a la conexión '{}'. Nivel actual: {}", tmKey, level_a);
            listeners.values().forEach(EventListener::onTransactionStart);
            return;
        }

        try {
            handle.begin();
            listeners.values().forEach(l -> {
                // Si el listener necesita contexto y define cómo crear un recurso,
                // se crea el recurso y se asocia al listener en su contexto antes de llamar a onBegin.
                if(l instanceof ContextAwareEventListener cl) {
                    Object resource = cl.createResource();
                    if(resource != null) {
                        getEventListenerContext(l).setResource(resource);
                    }
                }
                l.onBegin();
            });
            level_a++;
        } catch(DataAccessException e) {
            String message = "Transacción '%s': %s".formatted(tmKey, e.getMessage());
            logger.error(message, e.getCause());
            throw new DataAccessException(message, e);
        }
    }

    /**
     * Devuelve la lista de listeners en orden inverso al que fueron registrados, para asegurar que se notifique el fin de la transacción, el commit o el rollback en el orden correcto
     * @return La lista de listeners en orden inverso al que fueron registrados.
     */
    private List<EventListener> reverseListeners() {
        List<EventListener> listeners = new ArrayList<>(this.listeners.values());
        Collections.reverse(listeners);
        return listeners;
    }

    /**
     * Realiza el commit de la transacción. Si el nivel de anidamiento es mayor que 1, sólo decrementa el nivel y notifica a los listeners, pero no realiza el commit real hasta que se alcance el nivel 1.
     * @throws DataAccessException Si ocurre un error al realizar el commit.
     */
    public void commit() throws DataAccessException {
        if(level_a == 0) throw new IllegalStateException("La transacción no ha comenzado");

        boolean wasRoot = level_a == 1;

        if(!wasRoot) {
            logger.trace("Commit en transacción anidada asociada a la conexión '{}'. Nivel actual: {}", tmKey, level_a);
            reverseListeners().forEach(EventListener::onTransactionEnd);
        }

        level_a--;

        if(!wasRoot) return;

        DataAccessException commitException = null;

        try {
            handle.commit();
            reverseListeners().forEach(EventListener::onCommit);
        } catch(DataAccessException e) {
            commitException = e;
            try {
                handle.rollback();
            } catch(DataAccessException re) {
                e.addSuppressed(re);
            }
            String message = "Transacción '%s': %s".formatted(tmKey, e.getMessage());
            logger.error(message, e.getCause());
            throw new DataAccessException(message, e);
        }
        finally {
            try {
                handle.close();
            } catch(DataAccessException e) {
                if(commitException != null) {
                    commitException.addSuppressed(e);
                } else {
                    String message = "Transacción '%s': Error al cerrar la conexión tras commit".formatted(tmKey);
                    logger.error(message, e);
                    throw new DataAccessException(message, e);
                }
            }
        }
    }

    /**
     * Realiza el rollback de la transacción. Si el nivel de anidamiento es mayor que 1, sólo decrementa el nivel y notifica a los listeners, pero no realiza el rollback real hasta que se alcance el nivel 1.
     * @param cause La causa que originó el rollback.
     * @throws DataAccessException Si ocurre un error al realizar el rollback o si se facilitó una causa.
     */
    public void rollback(Throwable cause) throws DataAccessException {
        if(level_a == 0) throw new IllegalStateException("La transacción no ha comenzado");

        boolean wasRoot = level_a == 1;

        if(!wasRoot) {
            logger.trace("Rollback en transacción anidada asociada a la conexión '{}'. Nivel actual: {}", tmKey, level_a);
            reverseListeners().forEach(EventListener::onTransactionEnd);
        }

        level_a--;

        if(wasRoot) {
            DataAccessException rollbackException = null;

            try {
                handle.rollback();
                reverseListeners().forEach(EventListener::onRollback);
            } catch(DataAccessException e) {
                String message = "Transacción '%s': Error al hacer rollback: %s".formatted(tmKey, e.getMessage());
                logger.error(message, e.getCause());
                throw new DataAccessException(message, e);
            } finally {
                try {
                    handle.close();
                } catch(DataAccessException e) {
                    if(rollbackException != null) {
                        rollbackException.addSuppressed(e);
                    } else {
                        String message = "Transacción '%s': Error al cerrar la conexión tras rollback".formatted(tmKey);
                        logger.error(message, e);
                        throw new DataAccessException(message, e);
                    }
                }
            }
        }

        if(cause != null) {
            if(cause instanceof RuntimeException re) throw re;
            if(cause instanceof DataAccessException re) throw re;
            if(cause instanceof Error re) throw re;
            else throw new DataAccessException("Error en la transacción", cause);
        }
    }
}