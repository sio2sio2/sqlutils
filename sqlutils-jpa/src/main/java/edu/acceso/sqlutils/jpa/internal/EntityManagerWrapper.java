package edu.acceso.sqlutils.jpa.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy para EntityManager que intercepta llamadas a métodos específicos para evitar que el usuario pueda cerrar el EntityManager
 * o crear transacciones manualmente, lo cual podría interferir con el manejo de transacciones del framework.
 */
public class EntityManagerWrapper implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(EntityManagerWrapper.class);
    private final EntityManager em;

    private EntityManagerWrapper(EntityManager em) {
        this.em = em;
    }

    /**
     * Crea un proxy para el EntityManager proporcionado.
     * @param em El EntityManager que se quiere envolver.
     * @return El proxy del EntityManager.
     */
    public static EntityManager createProxy(EntityManager em) {
        Objects.requireNonNull(em);
        return (EntityManager) Proxy.newProxyInstance(
            em.getClass().getClassLoader(),
            new Class<?>[] { EntityManager.class },
            new EntityManagerWrapper(em)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            switch(method.getName()) {
                case "equals":
                    // Para equals, comparamos con el proxy o con el EntityManager real.
                    return proxy == args[0] || em.equals(args[0]);
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "getTransaction":
                    throw new UnsupportedOperationException("No puede obtenerse directamente un EntityTransaction de este EntityManager. Use el gestor de transacciones");
                case "close":
                    logger.trace("Capturada llamada a close(): el EntityManager no se cierra.");
                    return null;
                default:
                    return method.invoke(em, args);
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }
}
