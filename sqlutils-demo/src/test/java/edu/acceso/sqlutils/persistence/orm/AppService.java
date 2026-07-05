package edu.acceso.sqlutils.persistence.orm;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.orm.relations.FetchPlan;
import edu.acceso.sqlutils.orm.simple.crud.SimpleListCrud;
import edu.acceso.sqlutils.tx.event.LoggingManager;

/**
 * Implementación de la interfaz AppService utilizando un enfoque ORM simple.
 * Aplica un patrón de diseño Singleton para garantizar que solo exista una instancia de AppService.
 */
public class AppService implements edu.acceso.sqlutils.persistence.AppService {
    public static Logger logger = LoggerFactory.getLogger(AppService.class);

    private static AppService instance;

    private final Conexion cx;
    private final SimpleListCrud<Centro> centroDao;
    private final SimpleListCrud<Estudiante> estudianteDao;

    private AppService() throws IOException {
        cx = Conexion.create();
        centroDao = cx.getDao(Centro.class);
        estudianteDao = cx.getDao(Estudiante.class);
    }

    /**
     * Crea una instancia de AppService si aún no existe y la devuelve.
     * @return Una instancia de AppService.
     * @throws IOException Si ocurre un error al intentar abrir el guión SQL de inicialización de la base de datos.
     * @throws IllegalStateException Si ya existe una instancia de AppService.
     */
    public static AppService create() throws IOException {
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

    @Override
    public Optional<Centro> obtenerCentro(Long id) {
        return cx.transactionR(ctxt -> {
            Optional<Centro> centro = centroDao.get(id);
            logger.trace("Obtenido el centro con ID={}", id);
            return centro;
        });
    }

    @Override
    public List<Centro> listarCentros() {
        return cx.transactionR(ctxt -> {
            List<Centro> centros = centroDao.get();
            logger.trace("Obtenidos {} centros", centros.size());
            return centros;
        });
    }

    @Override
    public void agregarEstudiante(Estudiante estudiante) {
        cx.transaction(ctxt -> {
            LoggingManager lm = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);
            estudianteDao.insert(estudiante);
            lm.sendMessage(
                getClass(),
                Level.DEBUG,
                "Agregado el estudiante con ID=%s".formatted(estudiante.getId()),
                "Transacción fallida: no se pudo agregar el estudiante con ID=%s".formatted(estudiante.getId())
            );
        });
    }

    @Override
    public void agregarEstudiantes(Iterable<Estudiante> estudiantes) {
        List<Estudiante> lista = (estudiantes instanceof List) ? (List<Estudiante>) estudiantes : StreamSupport.stream(estudiantes.spliterator(), false).toList();

        cx.transaction(ctxt -> {
            LoggingManager lm = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);
            estudianteDao.insert(lista);
            lm.sendMessage(
                getClass(),
                Level.DEBUG,
                "Agregado %d estudiantes".formatted(lista.size()),
                "Transacción fallida: no se pudo agregar los %d estudiantes".formatted(lista.size())
            );
        });
    }

    @Override
    public void actualizarEstudiante(Estudiante estudiante) {
        cx.transaction(ctxt -> {
            LoggingManager lm = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);
            estudianteDao.update(estudiante);
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
            Optional<Estudiante> estudiante = estudianteDao.get(id);
            logger.trace("Obtenido el estudiante con ID={}", id);
            return estudiante;
        });
    }

    @Override
    public List<Estudiante> listarEstudiantes() {
        return cx.transactionR(ctxt -> {
            List<Estudiante> estudiantes = estudianteDao.get();
            logger.trace("Obtenidos {} estudiantes", estudiantes.size());
            return estudiantes;
        });
    }

    @Override
    public List<Estudiante> listarEstudiantesPerezosamente() {
        return cx.transactionR(ctxt -> {
            SimpleListCrud<Estudiante> estudianteDaoAnsioso = estudianteDao.with(cx.getDaoData().with(FetchPlan.LAZY));
            List<Estudiante> estudiantes = estudianteDaoAnsioso.get();
            logger.trace("Obtenidos {} estudiantes ansiosamente", estudiantes.size());
            return estudiantes;
        });
    }

    @Override
    public void operacionMultiple() {
        cx.transaction(ctxt -> {
            LoggingManager lm = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);
            Estudiante e1 = estudianteDao.get(1L).orElse(null);
            Estudiante e2 = estudianteDao.get(3L).orElse(null);

            e1.setNombre("Estudiante 1");
            estudianteDao.update(e1);
            lm.sendMessage(
                getClass(),
                Level.DEBUG,
                "Actualizado el estudiante con ID=%s".formatted(e1.getId()),
                "Transacción fallida: no se pudo actualizar el estudiante con ID=%s".formatted(e1.getId())
            );

            e2.setNombre("Estudiante 2");
            estudianteDao.update(e2);
            lm.sendMessage(
                getClass(),
                Level.DEBUG,
                "Actualizado el estudiante con ID=%s".formatted(e2.getId()),
                "Transacción fallida: no se pudo actualizar el estudiante con ID=%s".formatted(e2.getId())
            );
        });
    }
}
