package edu.acceso.sqlutils.persistence;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import edu.acceso.sqlutils.Config;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;

/**
 * Interfaz de servicio de aplicación para la gestión de entidades.
 * Como no son muchos los métodos, se definen todos en la misma intertfaz.
 * Es una interfaz y no una clase, porque se pretende ilustrar cómo resolver
 * la persistencia con varias implementaciones (JPA, ORM minimalista, etc.).
 */
public interface AppService {

    /**
     * Obtiene un centro por su identificador.
     * @param id El identificador del centro.
     * @return Un Optional que contiene el centro.
     */
    public Optional<Centro> obtenerCentro(Long id);
    /**
     * Agrega un estudiante a la base de datos.
     * @param estudiante El estudiante a agregar.
     */
    public void agregarEstudiante(Estudiante estudiante);
    /**
     * Agrega varios estudiantes a la base de datos.
     * @param estudiantes Los estudiantes a agregar.
     */
    public void agregarEstudiantes(Iterable<Estudiante> estudiantes);
    /**
     * Actualiza la información de un estudiante.
     * @param estudiante El estudiante a actualizar.
     */
    public void actualizarEstudiante(Estudiante estudiante);
    /**
     * Lista todos los centros.
     * @return Una lista con todos los centros.
     */
    public List<Centro> listarCentros();
    /**
     * Obtiene un estudiante por su identificador.
     * @param id El identificador del estudiante.
     * @return Un Optional que contiene el estudiante.
     */
    public Optional<Estudiante> obtenerEstudiante(Long id);
    /**
     * Lista todos los estudiantes.
     * @return Una lista con todos los estudiantes.
     */
    public List<Estudiante> listarEstudiantes();
    /**
     * Lista todos los estudiantes sin cargar sus centros.
     * @return Una lista con todos los estudiantes.
     */
    public List<Estudiante> listarEstudiantesPerezosamente();
    /**
     * Realiza una operación de prueba que involucra múltiples operaciones.
     * La operación se caracteriza por fallar en algún punto intermedio, para probar la gestión de transacciones.
     */
    public void operacionMultiple();

    /**
     * Fábrica de servicios de aplicación. Permite obtener la instancia del servicio de aplicación apropiado
     * según sea la configuración (JPA, ORM minimalista, etc.).
     * @return Una instancia de AppService.
     * @throws IOException Si ocurre un error al leer la configuración.
     */
    public static AppService factory() throws IOException {
        Config config = Config.get();

        if(config.getInput() == null) {
            try {
                return edu.acceso.sqlutils.persistence.jpa.AppService.create();
            } catch (IllegalStateException e) {
                return edu.acceso.sqlutils.persistence.jpa.AppService.get();
            }
        } else {
            try {
                return edu.acceso.sqlutils.persistence.orm.AppService.create();
            } catch (IllegalStateException e) {
                return edu.acceso.sqlutils.persistence.orm.AppService.get();
            }
        }
    }
}
