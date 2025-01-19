package edu.acceso.sqlutils.backend.sqlite;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.backend.Conexion;
import edu.acceso.sqlutils.dao.Crud;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.transaction.TransactionManager;

/**
 * Modela la conexión a una base de dato SQLite
 */
public class ConexionSqlite implements Conexion {
    final static Path esquema = Path.of(System.getProperty("user.dir"), "src", "test", "resources", "esquema.sql");
    final static String protocol = "jdbc:sqlite:";
    final static short maxConn = 10;
    final static short minConn = 1;

    private final HikariDataSource ds;

    /**
     * Constructor de la conexión.
     * TODO: Modificar el constructor para aplicar a la clase una especie de patrón Singleton:
     * Si la url+username+password coincide con una que ya se haya utilizado, no se crea un objeto
     * distinto, sino que se devuelve el objeto que se creó anteriormente.
     * @param opciones Las opciones de conexión.
     */
    public ConexionSqlite(Map<String, Object> opciones) throws DataAccessException {
        String path = (String) opciones.get("url");
        if(path == null) throw new IllegalArgumentException("No se ha fijado la url de la base de datos");

        String dbUrl = String.format("%s%s", protocol, path);
        Short maxConn = (Short) opciones.getOrDefault("maxconn", ConexionSqlite.maxConn);
        Short minConn = (Short) opciones.getOrDefault("minconn", ConexionSqlite.minConn);


        HikariConfig hconfig = new HikariConfig();
        hconfig.setJdbcUrl(dbUrl);
        hconfig.setMaximumPoolSize(maxConn);
        hconfig.setMinimumIdle(minConn);

        ds = new HikariDataSource(hconfig);

        initDB();
    }

    @Override
    public Crud<Centro> getCentroDao() {
        return new CentroSqlite(ds);
    }

    @Override
    public Crud<Estudiante> getEstudianteDao() {
        return new EstudianteSqlite(ds);
    }

    private void initDB() throws DataAccessException {
        try (Stream<Centro> centros = getCentroDao().get()) {
            centros.close();
        }
        // Si no podemos obtener la lista de los centros disponibles.
        // es porque aún no existe la base de datos.
        catch(DataAccessException err) {
            try (InputStream st = Files.newInputStream(esquema)) {
                SqlUtils.executeSQL(ds.getConnection(), st);
            }
            catch(SQLException e) {
                throw new DataAccessException("No puede crearse el esquema de la base de datos", e);
            }
            catch(IOException e) {
                throw new DataAccessException(String.format("No puede acceder al esquema: %s", esquema));
            }
        }
    }

    @Override
    public void transaccion(Transaccionable operaciones) throws DataAccessException {
        try(Connection conn = ds.getConnection()) {
            TransactionManager.transactionSQL(conn, c -> {
                operaciones.run(new CentroSqlite(c), new EstudianteSqlite(c));
            });
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }
}