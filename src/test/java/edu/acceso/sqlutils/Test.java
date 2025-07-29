package edu.acceso.sqlutils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import edu.acceso.sqlutils.backend.Backend;
import edu.acceso.sqlutils.crud.Crud;
import edu.acceso.sqlutils.dao.DaoFactory;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.query.SqlQuery;
import edu.acceso.sqlutils.query.SqlQueryFactory;

public class Test {
    private static final DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    @SuppressWarnings("unused")
    public static void main(String[] args) {
         SqlQueryFactory sqlQueryFactory = SqlQueryFactory.Builder.create()
                .register("sqlite", SqlQuery.class)
                .get();

        Config config = Config.create(args);

        Logger logger = (Logger) LoggerFactory.getLogger(Test.class);

        // De estas librerías externas no queremos mensajes de depuración.
        Logger reflectionLogger = (Logger) LoggerFactory.getLogger("org.reflections");
        Logger hikariLogger = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari");
        reflectionLogger.setLevel(Level.WARN);
        hikariLogger.setLevel(Level.WARN);
        logger.debug("Nivel de los registros de org.reflections establecido a {}", reflectionLogger.getLevel());

        DaoFactory daoFactory;
        try {
            daoFactory = Backend.createDaoFactory();
        } catch (DataAccessException e) {
            System.err.println("Error al inicializar el backend: " + e.getMessage());
            System.exit(1);
            throw new RuntimeException("Esto sólo sirve para que el compilador no se queje");
        }

        Crud<Centro> centroDao = daoFactory.getDao(Centro.class);
        Crud<Estudiante> estudianteDao = daoFactory.getDao(Estudiante.class);

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
            daoFactory.transaction(tx -> {
                Crud<Estudiante> eDao = tx.getDao(Estudiante.class); 
                Estudiante e1 = eDao.get(1L).orElse(null);
                Estudiante e2 = eDao.get(3L).orElse(null); // No existe.

                e1.setNombre("Estudiante 1");
                eDao.update(e1);

                e2.setNombre("Estudiante 2"); // Falla: RuntimeException
                eDao.update(e2);
            });
        }
        catch(Exception err) {
            System.err.printf("No se actualizan los dos estudiantes: %s\n", err.getMessage());
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
    }
}
