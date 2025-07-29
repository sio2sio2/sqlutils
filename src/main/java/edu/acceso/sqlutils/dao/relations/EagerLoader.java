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
 * Cargador inmediato de relaciones. Carga las relaciones de una entidad en cuanto carga la entidad.
 */
public class EagerLoader extends RelationLoader {

    /**
     * Constructor.
     * @param dao DAO a partir del cual se crea el cargador de relaciones
     */
    public EagerLoader(DaoCrud<? extends Entity> dao) {
        super(dao);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends Entity> E loadEntity(Class<E> entityClass, Long id) throws DataAccessException {
        if(id == null) return null; // Si no hay relación, no es necesario cargar nada.

        RelationEntity<E> relationEntity = new RelationEntity<>(entityClass, id);

        // Las referencias generan un ciclo infinito, así que devolvemos un proxy
        // en vez de la entidad directamente para romper el bucle.
        if(isLoaded(relationEntity)) {
            // Factoría efímera de proxies
            ProxyFactory factory = new ProxyFactory();
            factory.setSuperclass(entityClass);

            Class<?> proxyClass = factory.createClass();

            E proxyInstance;
            try {
                proxyInstance = (E) proxyClass.getDeclaredConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new DataAccessException(String.format("Error al crear proxy para '%s'. ¿Tiene un constructor sin argumentos?", entityClass.getSimpleName()), e);
            }

            MethodHandler handler = new LazyMethodHandler<>(getLoopBeginning());
            ((ProxyObject) proxyInstance).setHandler(handler);

            return proxyInstance;
        }
    
        registrar(relationEntity); // <-- Aquí se apunta en el historial (su entity aún es null)

        Crud<E> daoCrud = getDao(entityClass);
        E entity = daoCrud.get(id).orElse(null);   // <-- Aquí se carga la entidad relacionada.
        if(entity == null) throw new DataAccessException(String.format("Problema de integridad referencial. ID %d usando como clave foránea no existe", id));

        relationEntity.setLoadedEntity(entity);  // <-- Aquí se establece el valor de loadedEntity.
 
        return entity;
    }
 
    /**
     * Manejador de métodos que carga de forma perezosa la entidad relacionada que provoca
     * el ciclo infinito.
     * 
     * <p>Obsérvese una carga de entidades relacionadas que presenta un bucle:</p>
     * <pre>A -> B -> C -> D -> E -> C</pre>
     * <p>
     * Cuando se crea un ciclo infinito, es porque la entidad relacionada ("C" en el ejemplo) ya
     * se pretendió cargar en un punto anterior del historial y, por tanto, hay un
     * {@link RelationEntity} que en principio la tiene. Sin embargo, la carga inmediata supone que
     * recursivamente se carguen todas las entidades relacionadas y que no se genere realmente la
     * entidad, hasta que se haya resuelto la cadena de relaciones tras ella. En el ejemplo, para que
     * "C" se genere, es necesario que "D" y "E" lo hayan hecho antes. Pero "E" requiere
     * que "C" se haya generado, lo que provoca un ciclo infinito. Para resolverlo el "C" que se relaciona
     * con "E" se resuelve de forma perezosa mediante un proxy. Eso posibilita que se generen todas las entidades
     * (A, B, C, D, E). Luego, si en algún momento se accede a "C" desde "E", el proxy resuelve el problema
     * consultado el "C" que ya se había cargado en el historial de relaciones.
     * </p>
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