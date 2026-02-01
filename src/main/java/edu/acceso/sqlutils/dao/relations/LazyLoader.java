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
     */
    public LazyLoader(AbstractCrud<? extends Entity> originalDao, Class<E> entityClass) throws DataAccessException {
        super(originalDao, entityClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public E loadEntity(Long id) throws DataAccessException {
        // Si no hay relación, no es necesario cargar nada.
        if(id == null) return null;

        // Establecemos el RelationEntity asoaciado a este cargador
        setRl(id);

        if(checkAlreadyLoaded()) return (E) getRl().getLoadedEntity();

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

        MethodHandler handler = new LazyMethodHandler<>(getEntityClass(), id, this);
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

        /** Clase de la entidad relacionada */
        private final Class<E> entityClass;
        /** ID de la entidad relacionada (clave foránea en la entidad principal) */
        private final Long id;
        /** Cargador de relaciones */
        private final RelationLoader<E> loader;

        /** Entidad relacionada que se carga de forma perezosa y se almacena para que futuros accesos a la entidad no supongan una nueva carga */
        private E realEntity = null;

        public LazyMethodHandler(Class<E> entityClass, Long id, RelationLoader<E> loader) {
            if(id == null) throw new IllegalArgumentException("ID no puede ser nulo para LazyLoader. Si la clave foránea es nula, no use cargador.");

            this.entityClass = entityClass;
            this.id = id;
            this.loader = loader;
        }

        /**
         * Comprueba si la entidad relacionada ya ha sido cargada.
         * @return true si la entidad ya ha sido cargada, false en caso contrario
         */
        private boolean isLoaded() {
            // La entidad cargada nunca puede ser nula, porque el identificador no puede ser nulo.
            // Por consiguiente, que la entidad sea nula, indica que no se ha cargado aún.
            return realEntity != null;
        }

        @Override
        public Object invoke(Object self, Method method, Method proceed, Object[] args) throws Throwable {
            if(!isLoaded()) {
                realEntity = loader.dao.get(id).orElseThrow(
                    () -> new DataAccessException(String.format("Error de integridad referencial: no se encontró la entidad %s con ID %d", entityClass.getSimpleName(), id))
                );
                loader.getRl().setLoadedEntity(realEntity);
                logger.debug("Entidad '{}' con ID {} cargada de forma perezosa.", entityClass.getSimpleName(), id);
            }
            return method.invoke(realEntity, args);
        }
    }
}
