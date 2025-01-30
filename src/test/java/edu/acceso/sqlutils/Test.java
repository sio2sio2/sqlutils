package edu.acceso.sqlutils;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.stream.Stream;

import edu.acceso.sqlutils.backend.sqlite.CentroSqlite;
import edu.acceso.sqlutils.backend.sqlite.ConexionSqlite;
import edu.acceso.sqlutils.backend.sqlite.EstudianteSqlite;
import edu.acceso.sqlutils.dao.BackendFactory;
import edu.acceso.sqlutils.dao.Dao;
import edu.acceso.sqlutils.dao.DaoConnection;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;

public class Test {

    static DateTimeFormatter formato = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public static void main(String[] args) {
        // Estos datos debería obtenerse de alguna manera.
        Map<String, Object> opciones = Map.of(
            "base", "sqlite",
//            "url", Path.of(System.getProperty("java.io.tmpdir"), "caca.db").toString(),
            "url", "file::memory:?cache=shared",  // Para que se comparta la base de datos entre todas las conexiones del pool
            "user", "",
            "password", "",
            "esquema", Path.of(System.getProperty("user.dir"), "src", "test", "resources", "esquema.sql")
        );

        // Esta es el tipo de base de datos que usaremos para persistencia (sqlite)
        String base = (String) opciones.get("base");
        Path esquema = (Path) opciones.get("esquema");

        DaoConnection conexion = new BackendFactory()
            // Registramos todos los tipos de bases de datos soportados
            .register("SQLITE", ConexionSqlite.class)
            .register("MARIADB", null) // No hay soporte, pero se espera que lo haya en el futuro.  
            // Escogemos el conector adecuado e indicamos las clases DAO.
            .createConnection(base, opciones, esquema, CentroSqlite.class, EstudianteSqlite.class);

        // Obtenemos el objeto Dao que nos sirve para la persistencia de centros y estudiantes
        Dao dao = conexion.getDao();

        // Empezamos a operar usando el objeto anterior
        Centro castillo = null;
        try {
            castillo = dao.get(Centro.class, 11004866).orElse(null);
            System.out.println(castillo);
        }
        catch(DataAccessException err) {
            System.err.printf("No pueden obtenerse el centro del almacenamiento: %s", err.getMessage());
            System.exit(1);
        }

        // Creación de algunos estudiantes:
        Estudiante perico = null;
        try {
            Estudiante[] estudiantes = new Estudiante[] {
                new Estudiante(1, "Perico de los palotes", LocalDate.parse("10/12/1994", formato), castillo),
                new Estudiante(2, "María de la O", LocalDate.parse("23/04/1990", formato), castillo)
            };

            dao.insert(estudiantes);

            perico = dao.get(Estudiante.class, 1).orElse(null);
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
            if(dao.update(perico)) {
                // Lo recuperamos de la base de datos.
                perico = dao.get(Estudiante.class, 1).orElse(null);
                System.out.printf("-- \nHemos actualizado Perico: %s\n", perico);
            }
        }
        catch(DataAccessException err) {
            System.err.printf("No puede actualizarse el estudiante '%s': %s", perico, err.getMessage());
            System.exit(1);
        }

        // Ejemplo de transacción: intentamos actualizar ambos estudiantes.
        try {
            conexion.transaction(xDao -> {
                Estudiante e1 = xDao.get(Estudiante.class, 1).orElse(null);
                Estudiante e2 = xDao.get(Estudiante.class, 3).orElse(null); // No existe.

                e1.setNombre("Estudiante 1");
                xDao.update(e1);

                e2.setNombre("Estudiante 2"); // Falla: RuntimeException
                xDao.update(e2);
            });
        }
        catch(Exception err) {
            System.err.printf("No se actualizan los dos estudiantes: %s\n", err.getMessage());
        }

        // Comprobación de que ningún estudiante se actualizó
        System.out.println("-- \nLista de estudiantes:");
        try(Stream<Estudiante> estudiantes = dao.get(Estudiante.class)) {
            estudiantes.forEach(System.out::println);
        }
        catch(DataAccessException err) {
            System.err.printf("No puede obtenerse la lista de estudiantes: %s", err.getMessage());
            System.exit(1);
        }
    }
}