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
 * Cargador inmediato de relaciones. Carga las relaciones de una entidad en cuanto carga la entidad.
 * @param <E> Tipo de entidad que cargará el cargador de relaciones.
 */
public class EagerLoader<E extends Entity> extends RelationLoader<E> {
    private static final Logger logger = LoggerFactory.getLogger(EagerLoader.class);

    /**
     * Constructor.
     * @param originalDao DAO a partir del cual se crea el cargador de relaciones
     * @param entityClass Clase de la entidad que carga este cargador de relaciones
     */
    public EagerLoader(AbstractCrud<? extends Entity> originalDao, Class<E> entityClass) throws DataAccessException {
        super(originalDao, entityClass);
    }

    /**
     * Carga inmediata de la entidad relacionada.
     * @param id ID de la entidad relacionada.
     * 
     * <p>El problema de la carga inmediata es la creación de un ciclo infinito de referencias:</p>
     * <pre>A -> B -> C -> D -> E -> C</pre>
     * <p>
     * Cuando se crea un ciclo infinito, es se vuelve a cargar una entidad que ya se cargó anteriormente
     * ("C" en el ejemplo). La carga inmediata supone que recursivamente se carguen todas las entidades
     * relacionadas y que no se genere realmente la entidad, hasta que se haya resuelto toda la cadena
     * de relaciones tras ella. Por eso, en elk ejemplo, para que la entidad "C" se genere, es necesario que
     * priviamente lo hagan "D" y "E". Pero "E" requiere que "C" se haya generado antes, lo que provoca el
     * ciclo infinito. Para resolverlo, este segundo "C" se detecta como ya cargado en el historial de
     * relaciones y se resuelve de forma perezosa mediante el proxy {@link LazyMethodHandler}. Eso
     * posibilita que se generen todas las entidades (A, B, C, D, E). Luego, si en algún momento se accede
     * al segundo "C", el proxy resuelve el problema consultado el "C" que ya se había cargado en el
     * historial de relaciones.
     * </p>
     *
     */
    @SuppressWarnings("unchecked")
    @Override
    public E loadEntity(Long id) throws DataAccessException {
        if(id == null) return null; // Si no hay relación, no es necesario cargar nada.

        // Establecemos el RelationEntity asoaciado a este cargador
        setRl(id);

        // Las referencias generan un ciclo infinito, así que devolvemos un proxy
        // en vez de la entidad directamente para romper el bucle.
        if(checkAlreadyLoaded()) {
            // Factoría efímera de proxies
            ProxyFactory factory = new ProxyFactory();
            factory.setSuperclass(getEntityClass());

            Class<?> proxyClass = factory.createClass();

            E proxyInstance;
            try {
                proxyInstance = (E) proxyClass.getDeclaredConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new DataAccessException("Error al crear proxy para '%s'. ¿Tiene un constructor sin argumentos?".formatted(dao.getEntityClass().getSimpleName()), e);
            }

            MethodHandler handler = new LazyMethodHandler<>(getRl());
            ((ProxyObject) proxyInstance).setHandler(handler);

            logger.debug("Se genera un proxy para obtener entidad '{}' con ID {}", getEntityClass().getSimpleName(), id);
            return proxyInstance;
        }
    
        E entity = dao.get(id).orElse(null);   // <-- Aquí se carga la entidad relacionada.
        if(entity == null) throw new DataAccessException("Problema de integridad referencial. ID %d usando como clave foránea no existe".formatted(id));

        getRl().setLoadedEntity(entity);  // <-- Aquí se establece el valor de loadedEntity.
 
        return entity;
    }
 
    /**
     * Manejador de métodos que carga de forma perezosa la entidad relacionada que provoca
     * el ciclo infinito.
     */
    private static class LazyMethodHandler<E extends Entity> implements MethodHandler {
        private final RelationEntity<E> relationEntity;

        /**
         * Constructor.
         * @param relationEntity RelationEntity del historial que contiene la entidad relacionada.
         *    En este momento la entidad que contiene aún no ha sido cargada y es null.
         */
        public LazyMethodHandler(RelationEntity<E> relationEntity) {
            this.relationEntity = relationEntity;
        }

        @Override
        public Object invoke(Object self, Method method, Method proceed, Object[] args) throws Throwable {
            // Para cuando se requiera acceder a la entidad, el RelationEntity ya debe tenerla asignada.
            assert relationEntity.getLoadedEntity() != null : "La entidad relacionada no ha sido cargada aún.";
            return method.invoke(relationEntity.getLoadedEntity(), args);
        }
    }
}