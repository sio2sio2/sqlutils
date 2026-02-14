package edu.acceso.sqlutils.dao.relations;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.errors.DataAccessException;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;

/**
 * Cargador perezoso de relaciones. Carga las relaciones de una entidad bajo demanda, es decir, cuando se accede a ellas.
 * @param <E> Tipo de entidad que cargará el cargador de relaciones.
 */
public class LazyLoader<E extends Entity> extends RelationLoader<E> {
    private static final Logger logger = LoggerFactory.getLogger(LazyLoader.class);

    /**
     * Constructor.
     * @param originalDao DAO a partir del cual se crea el cargador de relaciones
     * @param entityClass Clase de la entidad que carga este cargador de relaciones
     * @throws DataAccessException Si no pueede crear el cargador de relaciones a partir del DAO proporcionado
     */
    public LazyLoader(AbstractCrud<? extends Entity> originalDao, Class<E> entityClass) throws DataAccessException {
        super(originalDao, entityClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected E loadEntityNotPreviouslyLoaded(Long id) throws DataAccessException {
        // Si no está cacheada, no puede estar en el historial de carga
        // if(isInHistory()) return (E) getRl().getLoadedEntity();

        // Factoría efímera de proxies
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(getEntityClass());

        Class<?> proxyClass = factory.createClass();

        E proxyInstance = null;

        try {
            proxyInstance = (E) proxyClass.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new DataAccessException("Error al crear proxy para la entidad %s. ¿Tiene un constructor sin argumentos?".formatted(getEntityClass().getSimpleName()), e);
        }

        MethodHandler handler = new LazyMethodHandler<>(this, id);
        ((ProxyObject) proxyInstance).setHandler(handler);

        return proxyInstance;
    }

    /**
     *  Manejador de métodos para la carga perezosa.
     * 
     * <p>
     * El manejador se basa en generar un proxy que carga de forma efectiva la entidad al solicitar
     * alguna de sus propiedades. De este modo, la carga de la entidad principal no supone la carga
     * de las entidades relacionadas.
     * </p>
     */
    private static class LazyMethodHandler<E extends Entity> implements MethodHandler {

        /** ID de la entidad relacionada (clave foránea en la entidad principal) */
        private final Long id;
        /** Cargador de relaciones */
        private final RelationLoader<E> loader;

        public LazyMethodHandler(RelationLoader<E> loader, Long id) {
            if(id == null) throw new IllegalArgumentException("ID no puede ser nulo para LazyLoader. Si la clave foránea es nula, no use cargador.");

            this.id = id;
            this.loader = loader;
        }

        @Override
        public Object invoke(Object self, Method method, Method proceed, Object[] args) throws Throwable {
            return switch(method.getName()) {
                case "getId" -> id;  // Para el ID no hace falta recuperar el objeto real.
                default -> {
                    if(!loader.isAlreadyLoaded()) {
                        E realEntity = loader.dao.get(id).orElseThrow(
                            () -> new DataAccessException(String.format("Error de integridad referencial: no se encontró la entidad %s con ID %d", loader.getEntityClass().getSimpleName(), id))
                        );
                        loader.getRl().setLoadedEntity(realEntity);
                        logger.debug("Entidad '{}' con ID {} cargada de forma perezosa.", loader.getEntityClass().getSimpleName(), id);
                    }
                    yield method.invoke(loader.getRl().getLoadedEntity(), args);
                }
            };
        }
    }
}