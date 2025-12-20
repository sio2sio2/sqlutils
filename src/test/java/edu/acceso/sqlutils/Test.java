package edu.acceso.sqlutils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import edu.acceso.sqlutils.backend.Conexion;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;

public class Test {
    private static Logger logger = (Logger) LoggerFactory.getLogger(Test.class);
    private static final DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    @SuppressWarnings("unused")
    public static void main(String[] args) {
        Config config = Config.create(args);

        // De estas librerías externas no queremos mensajes de depuración.
        Logger reflectionLogger = (Logger) LoggerFactory.getLogger("org.reflections");
        Logger hikariLogger = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");
        reflectionLogger.setLevel(Level.WARN);
        hikariLogger.setLevel(Level.WARN);

        logger.debug("Nivel de los registros de org.reflections establecido a {}", reflectionLogger.getLevel());

        Conexion conexion;
        try {
            conexion = Conexion.create();
        } catch (DataAccessException e) {
            logger.error("Error de conexión a la base de datos.", e);
            System.exit(1);
            throw new RuntimeException("Esto sólo sirve para que el compilador no se queje");
        } catch(IOException e) {
            logger.error("No se puede abrir el guion que inicializa la base de datos", e);
            System.exit(1);
            throw new RuntimeException("Esto sólo sirve para que el compilador no se queje");
        }

        var centroDao = conexion.getDao(Centro.class);
        var estudianteDao = conexion.getDao(Estudiante.class);

        Centro castillo = null;
        try {
            castillo = centroDao.get(11004866L).orElse(null);
            System.out.println(castillo);
        } catch (DataAccessException e) {
            System.err.println("Error al obtener el centro: " + e.getMessage());
        }

        Estudiante perico = null;;
        try {
            Estudiante[] estudiantes = new Estudiante[] {
                new Estudiante(1L, "Perico de los palotes", LocalDate.parse("10/12/1994", df), castillo),
                new Estudiante(2L, "María de la O", LocalDate.parse("23/04/1990", df), castillo)
            };

            estudianteDao.insert(estudiantes);

            perico = estudianteDao.get(1L).orElse(null);
            System.out.println("-- \nDatos de perico:");
            System.out.println(perico);
            System.out.println(perico.getCentro());
        }
        catch(DataAccessException err) {
            System.err.printf("No pueden almacenarse los estudiantes: %s", err.getMessage());
            System.exit(1);
        }

        // Actualización de un estudiante
        try {
            perico.setNombre("Perico de los Palotes");
            if(estudianteDao.update(perico)) {
                // Lo recuperamos de la base de datos.
                perico = estudianteDao.get(1L).orElse(null);
                System.out.printf("-- \nHemos actualizado Perico: %s\n", perico);
            }
        }
        catch(DataAccessException err) {
            System.err.printf("No puede actualizarse el estudiante '%s': %s", perico, err.getMessage());
            System.exit(1);
        }

        // Ejemplo de transacción: intentamos actualizar ambos estudiantes.
        try {
            conexion.transaction((cDao, eDao) -> {
                Estudiante e1 = eDao.get(1L).orElse(null);
                Estudiante e2 = eDao.get(3L).orElse(null); // No existe.

                e1.setNombre("Estudiante 1");
                eDao.update(e1);

                e2.setNombre("Estudiante 2"); // Falla: RuntimeException
                eDao.update(e2);
            });
        }
        catch(Exception err) {
            System.err.printf("No se actualizan nombres de estudiantes: %s\n", err.getMessage());
        }

        // Comprobación de que ningún estudiante se actualizó
        System.out.println("-- \nLista de estudiantes:");
        try(Stream<Estudiante> estudiantes = estudianteDao.getStream()) {
            estudiantes.forEach(System.out::println);
        }
        catch(DataAccessException err) {
            System.err.printf("No puede obtenerse la lista de estudiantes: %s", err.getMessage());
            System.exit(1);
        }

        // Listamos los centros existentes usando getList
        System.out.println("-- \nLista de centros:");
        try {
            List<Centro> centros = centroDao.getList();
            centros.forEach(System.out::println);
        } catch (DataAccessException e) {
            System.err.println("Error al obtener la lista de centros: " + e.getMessage());
        }
    }
}
