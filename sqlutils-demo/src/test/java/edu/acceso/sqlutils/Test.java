package edu.acceso.sqlutils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.persistence.AppService;

public class Test {
    private static Logger logger = (Logger) LoggerFactory.getLogger(Test.class);
    private static final DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public static void main(String[] args) {
        Config config = Config.create(args);

        Logger packageLogger = (Logger) LoggerFactory.getLogger(Test.class.getPackageName());
        packageLogger.setLevel(config.getLogLevel());

        AppService appService;
        try {
            appService = AppService.factory();
        } catch (DataAccessException e) {
            logger.error("Error de conexión a la base de datos.", e);
            System.exit(1);
            throw new RuntimeException("Esto sólo sirve para que el compilador no se queje");
        } catch(IOException e) {
            logger.error("No se puede abrir el guion que inicializa la base de datos", e);
            System.exit(1);
            throw new RuntimeException("Esto sólo sirve para que el compilador no se queje");
        }

        Centro castillo = null;
        try {
            castillo = appService.obtenerCentro(11004866L).orElse(null);
            System.out.println(castillo);
        } catch (DataAccessException e) {
            System.err.println("Error al obtener el centro: " + e.getMessage());
        }

        
        try {
            List<Estudiante> estudiantes = List.of(
                new Estudiante(1L, "Perico de los palotes", LocalDate.parse("10/12/1994", df), castillo),
                new Estudiante(2L, "María de la O", LocalDate.parse("23/04/1990", df), castillo)
            );

            appService.agregarEstudiantes(estudiantes);
            Estudiante perico = appService.obtenerEstudiante(1L).orElse(null);
            System.out.println("-- \nDatos de perico:");
            System.out.println(perico);
        }
        catch(DataAccessException err) {
            System.err.printf("No pueden almacenarse los estudiantes: %s", err.getMessage());
            System.exit(1);
        }

        // Actualización de un estudiante
        try {
            Estudiante perico = appService.obtenerEstudiante(1L).orElse(null);
            perico.setNombre("Perico de los Palotes");
            appService.actualizarEstudiante(perico);
            perico = appService.obtenerEstudiante(1L).orElse(null);
            System.out.printf("-- \nHemos actualizado Perico: %s\n", perico);
        }
        catch(DataAccessException err) {
            System.err.printf("No puede actualizarse el estudiante: %s.\n", err.getMessage());
            System.exit(1);
        }

        // Ejemplo de transacción: intentamos actualizar ambos estudiantes.
        try {
            appService.operacionMultiple();
        }
        catch(Exception err) {
            System.err.printf("No se actualizan nombres de estudiantes: %s.\n", err.getMessage());
        }

        // Comprobación de que ningún estudiante se actualizó
        System.out.println("-- \nLista de estudiantes (falla por carga perezosa):");
        try {
            List<Estudiante> estudiantes = appService.listarEstudiantesPerezosamente();
            estudiantes.forEach(System.out::println);
        }
        catch(DataAccessException err) {
            System.err.printf("No puede obtenerse la lista de estudiantes: %s", err.getMessage());
        }

        System.out.println("-- \nLista de estudiantes (carga ansiosa):");
        try {
            List<Estudiante> estudiantes = appService.listarEstudiantes();
            estudiantes.forEach(System.out::println);
        } catch (DataAccessException e) {
            System.err.println("Error al obtener la lista de estudiantes: " + e.getMessage());
        }

    }
}
