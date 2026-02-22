package edu.acceso.sqlutils.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.ConnectionWrapper;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Clase que representa el contexto completo de una transacción
 * (qué conexión usa, qué nivel de anidamiento tiene, etc.)
 */
class Transaction {
    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    private final Connection conn;
    private int level_a;
    private final boolean originalAutoCommit;
    private final Map<String, EventListener> listeners;
    private final String tmKey;

    private final Map<EventListener, Object> resources_a = new HashMap<>();

    public Transaction(String key, Connection conn, Map<String, EventListener> listeners) throws SQLException {
        Objects.requireNonNull(conn, "La transacción debe tener una conexión asociada");

        this.conn = conn ;
        this.originalAutoCommit = conn.getAutoCommit();
        this.listeners = listeners;
        this.tmKey = key;
        this.level_a = 0;
    }

    int level() {
        return level_a;
    }

    EventListenerContext getEventListenerContext(EventListener listener) {
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

    TransactionContext getContext() {
        return new TransactionContext() {
            @Override
            public int level() {
                return level_a;
            }

            @Override
            public String key() {
                return tmKey;
            }

            @Override
            public Connection connection() {
                return ConnectionWrapper.createProxy(conn);
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

    void begin() throws DataAccessException {
        // La transacción sólo empieza realmente en el primer nivel de anidamiento.
        if(level_a++ > 0) {
            logger.trace("Transacción anidada asociada a la conexión '{}'. Nivel actual: {}", tmKey, level_a);
            listeners.values().forEach(EventListener::onTransactionStart);
            return;
        }

        try {
            conn.setAutoCommit(false);
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
        } catch(SQLException e) {
            logger.error("Error al iniciar la transacción asociada a la conexión '{}'", tmKey, e);
            throw new DataAccessException("Error al iniciar la transacción", e);
        }
    }

    private List<EventListener> reverseListeners() {
        List<EventListener> listeners = new ArrayList<>(this.listeners.values());
        Collections.reverse(listeners);
        return listeners;
    }

    void commit() throws DataAccessException {
        boolean wasRoot = level_a == 1;

        if(level_a == 0) throw new IllegalStateException("La transacción no ha comenzado");

        if(!wasRoot) {
            logger.trace("Commit en transacción anidada asociada a la conexión '{}'. Nivel actual: {}", tmKey, level_a);
            reverseListeners().forEach(EventListener::onTransactionEnd);
        }

        level_a--;

        if(!wasRoot) return;

        SQLException commitException = null;

        try {
            conn.commit();
            reverseListeners().forEach(EventListener::onCommit);
        } catch(SQLException e) {
            logger.error("Error al hacer commit en la transacción asociada a la conexión '{}'", tmKey, e);
            commitException = e;
            throw new DataAccessException("Error al hacer commit en la transacción", e);
        } finally {
            try {
                conn.setAutoCommit(originalAutoCommit);
                conn.close();
            } catch(SQLException e) {
                logger.error("Error al cerrar la transacción tras commit en la transacción asociada a la conexión '{}'", tmKey, e);
                if(commitException != null) {
                    commitException.addSuppressed(e);
                } else {
                    throw new DataAccessException("Error al cerrar la transacción tras commit", e);
                }
            }
        }
    }

    void rollback(Throwable cause) throws DataAccessException {
        boolean wasRoot = level_a == 1;

        if(level_a == 0) throw new IllegalStateException("La transacción no ha comenzado");

        if(!wasRoot) {
            logger.trace("Rollback en transacción anidada asociada a la conexión '{}'. Nivel actual: {}", tmKey, level_a);
            reverseListeners().forEach(EventListener::onTransactionEnd);
        }

        level_a--;

        SQLException rollbackException = null;

        if(wasRoot) {
            try {
                conn.rollback();
                reverseListeners().forEach(EventListener::onRollback);
            } catch(SQLException e) {
                logger.error("Error al hacer rollback en la transacción asociada a la conexión '{}'", tmKey, e);
                rollbackException = e;
                throw new DataAccessException("Error al hacer rollback en la transacción", e);
            } finally {
                try {
                    conn.setAutoCommit(originalAutoCommit);
                    conn.close();
                } catch(SQLException e) {
                    logger.error("Error al cerrar la transacción tras rollback en la transacción asociada a la conexión '{}'", tmKey, e);
                    if(rollbackException != null) {
                        rollbackException.addSuppressed(e);
                    } else {
                        throw new DataAccessException("Error al cerrar la transacción tras rollback", e);
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