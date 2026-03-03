package edu.acceso.sqlutils.orm.relations;

import java.lang.reflect.Method;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.orm.AbstractCrud;
import edu.acceso.sqlutils.orm.DaoFactory;
import edu.acceso.sqlutils.orm.DaoFactory.DaoData;
import edu.acceso.sqlutils.orm.minimal.Entity;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;

/**
 * Cargador de relaciones.
 * Proporciona métodos comunes para cargar entidades relacionadas.
 * 
 * @param <E> Tipo de entidad que este cargador se encargará de cargar.
 * 
 * <p>Contiene una referencia al cargador de relaciones previo que originó el DAO que creó este cargador.
 * De este modo, se puede mantener un historial de entidades cargadas para detectar ciclos infinitos.</p>
 */
public class RelationLoader<E extends Entity> {
    private static final Logger logger = LoggerFactory.getLogger(RelationLoader.class);

    /** Cargador de relaciones que cargó la entidad que usa este cargador */
    protected final RelationLoader<? extends Entity> previous;

    /**
     * Datos característicos del DAO relacionado con esta carga de relaciones.
     */
    protected final DaoData data;

    /** Entidad que carga este cargador */
    protected RelationEntity<E> rl;

    /** Clase de la entidad que carga este cargador de relaciones */
    protected final Class<E> entityClass;

    /**
     * Construye un nuevo cargador de relaciones a partir de otro previo.
     * @param entityClass Clase de la entidad que carga este cargador de relaciones
     * @param previous Cargador de relaciones previo
     * @throws DataAccessException Si no pueede crear el cargador de relaciones a partir del cargador previo proporcionado
     */
    public RelationLoader(Class<E> entityClass, RelationLoader<? extends Entity> previous) throws DataAccessException {
        Objects.requireNonNull(previous, "Este constructor sólo puede usarse para crear cargadores originadas por algún otro cargador previo.");
        this.data = previous.data;
        this.entityClass = entityClass;
        this.previous = previous;
        logger.debug("Creado cargador de relaciones para una entidad '{}'", entityClass.getSimpleName());
    }

    /**
     * Construye el primer cargador de una cadena de relaciones usando los datos del DAO a partir del cual se crea.
     * @param entityClass Clase de la entidad que carga este cargador de relaciones
     * @param data Datos característicos del DAO relacionado con esta carga de relaciones
     */
    public RelationLoader(Class<E> entityClass, DaoData data) {
        this.data = data;
        this.entityClass = entityClass;
        this.previous = null;
        logger.debug("Creado cargador de relaciones para una entidad '{}' con clave de DAO '{}'", entityClass.getSimpleName(), data.key());
    }

    /** Calcula la profundidad de este cargador de relaciones en el historial de relaciones. */
    private int getDepth() {
        int depth = 0;
        RelationLoader<? extends Entity> current = this.previous;
        while(current != null) {
            depth++;
            current = current.previous;
        }
        return depth;
    }

    /**
     * Obtiene la clase de la entidad que carga este cargador de relaciones.
     * 
     * @return Clase de la entidad
     */
    public Class<E> getEntityClass() {
        return entityClass;
    }

    /**
     * Obtiene los datos característicos del DAO relacionado con esta carga de relaciones.
     * @return Los datos solicitados.
     */
    public DaoData getData() {
        return data;
    }

    /**
     * Carga la entidad correspondiente a una clave foránea.
     * 
     * @param id Clave foránea que identifica la entidad o {@code null} si no hay relación.
     * @return Entidad cargada o {@code null} si no hay relación
     * @throws DataAccessException Si ocurre un error al acceder a los datos
     */
    public E loadEntity(Long id) throws DataAccessException {
        // Si no hay relación, no es necesario cargar nada.
        if(id == null) return null;

        FetchPlan fetchPlan = data.fetchPlan();

        // Establecemos el RelationEntity asoaciado a este cargador
        setRl(id);

        // Comprobamos si la entidad ya está en caché
        E cachedEntity = data.cacheManager().get(getEntityClass(), id);
        if(cachedEntity != null) return cachedEntity;

        boolean shouldLoadEagerly = fetchPlan.depth() == null || fetchPlan.depth() > getDepth();

        if(shouldLoadEagerly) {
            // Con carga ansiosa existe en problema de los ciclos infinitos:
            // A menos que esté ya en el historial de relaciones, se procede a cargar
            // la entidad relacionada. Si la siguiente entidad, también tiene relación
            // se hará lo mismo, y así sucesivamente. Las cargas paran cuando la entidad
            // cargada ya no tiene relaciones o bien cuando se detecta que la entidad cargada
            // está en el historial. Entonces, se va saliendo de la recursividad y estableciéndose
            // los valores de las entidades relacionadas.
            if(isInHistory()) return createProxy(getRl());  // Devolvemos un proxy que reutiliza el relationEntity que se encontró en el historial.
            else {
                AbstractCrud<E> dao = DaoFactory.get(data.key()).getDao(this);
                E entity = dao.get(id).orElse(null);   // <-- Aquí se carga la entidad relacionada (que podría a su vez llamar a loadEntity).
                if(entity == null) throw new DataAccessException("Problema de integridad referencial. ID %d usando como clave foránea no existe".formatted(id));

                getRl().setLoadedEntity(entity);  // <-- Aquí se establece el valor de loadedEntity.
                return entity;
            }
        }
        else return createProxy(getRl()); // Con carga perezosa, siempre se crea un proxy. 
    }

    /**
     * Comprueba si el cargador ya ha cargado la entidad asociada y, en caso de que no lo haya hecho.
     * detecta si la entidad ya se había cargado por algún cargador previo. Si fue así,
     * copia la entidad cargada del cargador previo en el actual.
     * 
     * @param relationEntity Entidad a comprobar
     * @return {@code true} si es la primera de un ciclo, {@code false} en caso contrario
     */
    @SuppressWarnings("unchecked")
    protected boolean isInHistory() {
        RelationEntity<E> relationEntity = this.getRl();
        RelationLoader<? extends Entity> previous = this.previous;

        if(isAlreadyLoaded()) return true;

        while(previous != null) {
            if(relationEntity.equals(previous.getRl())) {
                relationEntity.setLoadedEntity((E) previous.getRl().getLoadedEntity());
                logger.debug("Detectado ciclo de referencias para la entidad '{}' con ID {}", 
                    relationEntity.getEntityClass().getSimpleName(), relationEntity.getId());
                return true;
            }
            previous = previous.previous;
        }

        return false;
    }

    /**
     * Informa de si el cargador ya cargó la entidad asociada.
     * @return {@code true} si la entidad ya ha sido cargada, {@code false} en caso contrario
     */
    protected boolean isAlreadyLoaded() {
        RelationEntity<E> relationEntity = this.getRl();
        return relationEntity.getLoadedEntity() != null;
    }

    /**
     * Establece la entidad asociada a este cargador de relaciones.
     * 
     * @param id ID de la entidad
     */
    protected void setRl(Long id) {
        this.rl = new RelationEntity<>(getEntityClass(), id);
    }

    /**
     * Obtiene la entidad asociada a este cargador de relaciones.
     * 
     * @return La entidad solicitada.
     */
    protected RelationEntity<E> getRl() {
        if(rl == null) throw new IllegalStateException("No se ha establecido la entidad asociada a este cargador de relaciones.");
        return rl;
    }
    
    /**
     * Crea un proxy para la entidad relacionada que carga este cargador de relaciones.
     * @param relationEntity RelationEntity que contiene la información de la entidad relacionada a cargar.
     * @throws DataAccessException Si ocurre un error al crear el proxy.
     * @return El proxy creado.
     */
    @SuppressWarnings("unchecked")
    private E createProxy(RelationEntity<E> relationEntity) throws DataAccessException {
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

        MethodHandler handler = new LazyMethodHandler<>(this);
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

        /** Cargador de relaciones */
        private final RelationLoader<E> loader;

        public LazyMethodHandler(RelationLoader<E> loader) {
            if(loader.getRl().getId() == null) throw new IllegalStateException("No se ha establecido el identificador de la entidad asociada a este cargador de relaciones.");
            this.loader = loader;
        }

        @Override
        public Object invoke(Object self, Method method, Method proceed, Object[] args) throws Throwable {
            RelationEntity<E> rl = loader.getRl();

            return switch(method.getName()) {
                case "getId" -> rl.getId();  // Para el ID no hace falta recuperar el objeto real.
                default -> {
                    if(!loader.isAlreadyLoaded()) {
                        if(!loader.data.tm().isActive()) {
                            throw new DataAccessException("Evaluación perezosa fallida: no hay transacción activa para cargar la entidad '%s' con ID %d".formatted(loader.getEntityClass().getSimpleName(), rl.getId()));
                        }
                        AbstractCrud<E> dao = DaoFactory.get(loader.data.key()).getDao(loader);
                        E realEntity = dao.get(rl.getId()).orElseThrow(
                            () -> new DataAccessException(String.format("Error de integridad referencial: no se encontró la entidad %s con ID %d", loader.getEntityClass().getSimpleName(), rl.getId()))
                        );
                        rl.setLoadedEntity(realEntity);
                        logger.debug("Entidad '{}' con ID {} cargada de forma perezosa.", loader.getEntityClass().getSimpleName(), rl.getId());
                    }
                    yield method.invoke(rl.getLoadedEntity(), args);
                }
            };
        }
    }
}