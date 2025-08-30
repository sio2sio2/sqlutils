package edu.acceso.sqlutils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import edu.acceso.sqlutils.backend.Backend;
import edu.acceso.sqlutils.dao.DaoFactory;
import edu.acceso.sqlutils.dao.crud.SqlQueryFactory;
import edu.acceso.sqlutils.dao.crud.simple.SimpleListCrud;
import edu.acceso.sqlutils.dao.crud.simple.SimpleSqlQueryGeneric;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;

public class Test {
    private static final DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    @SuppressWarnings("unused")
    public static void main(String[] args) {
        // Juego de instancias SqlQuery para definir las consultas SQL.
         SqlQueryFactory sqlQueryFactory = SqlQueryFactory.Builder.create("centro")
                // Ejemplo de cómo registrar las consultas para SQLite.
                .register("sqlite", SimpleSqlQueryGeneric.class)
                // Para todos los demás SGBD, se utiliza la misma, de modo
                // que el registro específico para SQLite sobraba, pero ha servido
                // para ilustrar cómo se hace.
                .register("*", SimpleSqlQueryGeneric.class)
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

        SimpleListCrud<Centro> centroDao = (SimpleListCrud<Centro>) daoFactory.getDao(Centro.class);
        SimpleListCrud<Estudiante> estudianteDao = (SimpleListCrud<Estudiante>) daoFactory.getDao(Estudiante.class);

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
                SimpleListCrud<Estudiante> eDao = (SimpleListCrud<Estudiante>) tx.getDao(Estudiante.class);
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
