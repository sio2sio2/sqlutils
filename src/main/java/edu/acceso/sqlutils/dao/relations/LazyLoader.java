package edu.acceso.sqlutils.dao.relations;

import java.lang.reflect.Method;

import edu.acceso.sqlutils.crud.Crud;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.DaoCrud;
import edu.acceso.sqlutils.errors.DataAccessException;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;

/**
 * Cargador perezoso de relaciones. Carga las relaciones de una entidad bajo demanda, es decir, cuando se accede a ellas.
 */
public class LazyLoader extends RelationLoader {

    /**
     * Constructor.
     * @param dao DAO a partir del cual se crea el cargador de relaciones
     */
    public LazyLoader(DaoCrud<? extends Entity> dao) {
        super(dao);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends Entity> E loadEntity(Class<E> entityClass, Long id) throws DataAccessException {
        // Si no hay relación, no es necesario cargar nada.
        if(id == null) return null;

        // Si la entidad ya se cargó (ciclo), se devuelve directamente.
        // (Lo evitamos para evitar devolver información obsoleta)
        // RelationEntity<E> relationEntity = new RelationEntity<>(entityClass, id);
        // if(isLoaded(relationEntity)) return (E) getLoopBeginning().getLoadedEntity();

        // Factoría efímera de proxies
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(entityClass);

        Class<?> proxyClass = factory.createClass();

        E proxyInstance = null;

        try {
            proxyInstance = (E) proxyClass.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new DataAccessException(String.format("Error al crear proxy para la entidad %s. ¿Tiene un constructor sin argumentos?", entityClass.getSimpleName()), e);
        }

        MethodHandler handler = new LazyMethodHandler<>(entityClass, id, this);
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
        private final RelationLoader loader;

        /** Entidad relacionada que se carga de forma perezosa y se almacena para que futuros acceso a la entidad no supongan una nueva carga */
        private E realEntity = null;

        public LazyMethodHandler(Class<E> entityClass, Long id, RelationLoader loader) {
            if(id == null) throw new IllegalArgumentException("ID no puede ser nulo para LazyLoader. Si la clave foránea es nula, no use cargador.");

            this.entityClass = entityClass;;
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
                Crud<E> dao = loader.getDao(entityClass);
                realEntity = dao.get(id).orElseThrow(
                    () -> new DataAccessException(String.format("Error de integridad referencial: no se encontró la entidad %s con ID %d", entityClass.getSimpleName(), id))
                );
                RelationEntity<E> relationEntity = new RelationEntity<>(entityClass, id);
                relationEntity.setLoadedEntity(realEntity);
                loader.registrar(relationEntity);
            }
            return method.invoke(realEntity, args);
        }
    }
}
