package edu.acceso.sqlutils.persistence.jpa;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.tx.event.LoggingManager;
import jakarta.persistence.EntityManager;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.metamodel.SingularAttribute;

/**
 * Implementación de la interfaz AppService utilizando JPA para la persistencia de datos.
 * Aplica un patrón de diseño Singleton para garantizar que solo exista una instancia de AppService.
 */
public class AppService implements edu.acceso.sqlutils.persistence.AppService {
    private static Logger logger = LoggerFactory.getLogger(AppService.class);

    private static AppService instance;
    private final Conexion cx;

    private AppService() {
        // Constructor privado para evitar instanciación directa
        this.cx = Conexion.create();
    }

    /**
     * Crea una instancia de AppService si aún no existe y la devuelve.
     * @return Una instancia de AppService.
     * @throws IllegalStateException Si ya existe una instancia de AppService.
     */
    public static AppService create() {
        if (instance != null) {
            throw new IllegalStateException("AppService ya ha sido creado");
        }

        instance = new AppService();
        return instance;
    }

    /**
     * Devuelve la instancia existente de AppService.
     * @return La instancia existente de AppService.
     * @throws IllegalStateException Si no existe una instancia de AppService.
     */
    public static AppService get() {
        if (instance == null) {
            throw new IllegalStateException("AppService no ha sido creado. Llama a create() primero.");
        }
        return instance;
    }

    /**
     * Obtiene todas las entidades de una clase dada.
     * 
     * <p>Equivale al SQL:</p>
     * <pre>SELECT e.* FROM Entity e</pre>
     * @param <T> Tipo de la entidad.
     * @param em El EntityManager.
     * @param entityClass La clase de la entidad.
     * @return Una lista de todas las entidades de la clase dada.
     */
    private static <T> List<T> getAll(EntityManager em, Class<T> entityClass) {
        CriteriaBuilder cb = em.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = cb.createQuery(entityClass);
        Root<T> entity = criteriaQuery.from(entityClass);
        criteriaQuery.select(entity);
        List<T> result = em.createQuery(criteriaQuery).getResultList();
        logger.debug("Se han recuperado {} registros de la entidad {}", result.size(), entityClass.getSimpleName());
        return result;
    }

    /**
     * Obtiene las entidades de una clase dada que cumplen uno de sus atributos es igual a un valor dado.
     * @param <T> Tipo de la entidad.
     * @param <V> Tipo del atributo.
     * @param em El EntityManager.
     * @param entityClass La clase de la entidad.
     * @param attribute El atributo por el que filtrar.
     * @param value El valor del atributo por el que filtrar.
     * @return Una lista de entidades que cumplen la condición.
     */
    private static <T, V> List<T> getWhere(EntityManager em, Class<T> entityClass, SingularAttribute<? super T, V> attribute, V value) {
        CriteriaBuilder cb = em.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = cb.createQuery(entityClass);
        Root<T> entity = criteriaQuery.from(entityClass);
        criteriaQuery.select(entity).where(cb.equal(entity.get(attribute), value));
        List<T> result = em.createQuery(criteriaQuery).getResultList();
        logger.debug("Se han recuperado {} registros de la entidad {}", result.size(), entityClass.getSimpleName());
        return result;
    }


    @Override
    public Optional<Centro> obtenerCentro(Long id) {
        return cx.transactionR(ctxt -> {
            EntityManager em = ctxt.handle();
            Optional<Centro> centro = Optional.ofNullable(em.find(Centro.class, id));

            if(centro.isPresent()) logger.trace("Obtenido el centro con ID={}", id);
            else logger.trace("No se encontró el centro con ID={}", id);

            return centro;
        });
    }

    @Override
    public List<Centro> listarCentros() {
        return cx.transactionR(ctxt -> {
            EntityManager em = ctxt.handle();
            List<Centro> centros = getAll(em, Centro.class);
            logger.trace("Obtenidos {} centros", centros.size());
            return centros;
        });
    }

    @Override
    public void agregarEstudiante(Estudiante estudiante) {
        cx.transaction(ctxt -> {
            EntityManager em = ctxt.handle();
            em.persist(estudiante);
            logger.trace("Agregado el estudiante con ID={}", estudiante.getId());
        });
    }

    @Override
    public void agregarEstudiantes(Iterable<Estudiante> estudiantes) {
        cx.transaction(ctxt -> {
            EntityManager em = ctxt.handle();
            LoggingManager lm = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);

            for(Estudiante estudiante : estudiantes) {
                if(estudiante.getCentro() != null) em.merge(estudiante.getCentro()); // Asegurarse de que el centro esté gestionado
                em.persist(estudiante);
                lm.sendMessage(
                    getClass(),
                    Level.DEBUG,
                    "Agregado el estudiante con ID=%s".formatted(estudiante.getId()),
                    "Transacción fallida: no se pudo agregar el estudiante con ID=%s".formatted(estudiante.getId())
                );
            }
        });
    }

    @Override
    public void actualizarEstudiante(Estudiante estudiante) {
        cx.transaction(ctxt -> {
            EntityManager em = ctxt.handle();
            LoggingManager lm = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);

            em.merge(estudiante);
            lm.sendMessage(
                getClass(),
                Level.DEBUG,
                "Actualizado el estudiante con ID=%s".formatted(estudiante.getId()),
                "Transacción fallida: no se pudo actualizar el estudiante con ID=%s".formatted(estudiante.getId())
            );
        });
    }

    @Override
    public Optional<Estudiante> obtenerEstudiante(Long id) {
        return cx.transactionR(ctxt -> {
            EntityManager em = ctxt.handle();
            Optional<Estudiante> estudiante = Optional.ofNullable(em.find(Estudiante.class, id));

            if(estudiante.isPresent()) logger.trace("Obtenido el estudiante con ID={}", id);
            else logger.trace("No se encontró el estudiante con ID={}", id);

            return estudiante;
        });
    }

    @Override
    public List<Estudiante> listarEstudiantes() {
        return cx.transactionR(ctxt -> {
            EntityManager em = ctxt.handle();
            List<Estudiante> estudiantes = getAll(em, Estudiante.class);
            logger.trace("Obtenidos {} estudiantes", estudiantes.size());
            return estudiantes;
        });
    }

    @Override
    public List<Estudiante> listarEstudiantesPerezosamente() {
        logger.debug("No se implementa este método particularmente");
        return listarEstudiantes();
    }

    @Override
    public void operacionMultiple() {
        cx.transaction(ctxt -> {
            EntityManager em = ctxt.handle();
            LoggingManager lm = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);

            Estudiante e1 = em.find(Estudiante.class, 1L);
            Estudiante e2 = em.find(Estudiante.class, 3L);

            e1.setNombre("Estudiante 1");
            em.merge(e1);
            lm.sendMessage(
                getClass(),
                Level.DEBUG,
                "Actualizado el estudiante con ID=%s".formatted(e1.getId()),
                "Transacción fallida: no se pudo actualizar el estudiante con ID=%s".formatted(e1.getId())
            );

            e2.setNombre("Estudiante 2");
            em.merge(e2);
            lm.sendMessage(
                getClass(),
                Level.DEBUG,
                "Actualizado el estudiante con ID=%s".formatted(e2.getId()),
                "Transacción fallida: no se pudo actualizar el estudiante con ID=%s".formatted(e2.getId())
            );
        });
    }
}
