package edu.acceso.sqlutils.backend.sqlite;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import edu.acceso.sqlutils.dao.AbstractDao;
import edu.acceso.sqlutils.dao.ConnectionProvider;
import edu.acceso.sqlutils.dao.Crud;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.transaction.TransactionManager;
import edu.acceso.sqlutils.FkLazyLoader;
import edu.acceso.sqlutils.SqlUtils;

/**
 * Modela para un Estudiante las operaciones de acceso a una base de datos SQLite.
 */
public class EstudianteSqlite extends AbstractDao implements Crud<Estudiante> {

    /**
     * Constructor de la clase.
     * @param ds Fuente de datos.
     */
    public EstudianteSqlite(DataSource ds) {
        super(ds);
    }

    /**
     * Constructor de la clase.
     * @param conn Conexión de datos.
     */
    public EstudianteSqlite(Connection conn) {
        super(conn);
    }

    /**
     * Constructor de la clase.
     * @param cp Proveedor de la conexión.
     */
    public EstudianteSqlite(ConnectionProvider cp) {
        super(cp);
    }

    /**
     * Recupera los datos de un registro de la tabla para convertirlos en objeto Estudiante.
     * La obtención del centro al que está adscrito el alumno es perezosa: se apunta el identificador
     * y no se obtiene el centro en sí hasta que no se use el getter correspondiente.
     * 
     * @param rs El ResultSet que contiene el registro.
     * @return Un objeto Estudiante que modela los datos del registro.
     * @throws SQLException Cuando se produce un error al recuperar los datos del registro.
     */
    private static Estudiante resultToEstudiante(ResultSet rs, ConnectionProvider cp) throws SQLException {
        int id = rs.getInt("id_estudiante");
        String nombre = rs.getString("nombre");
        Integer idCentro = rs.getInt("centro");
        if(rs.wasNull()) idCentro = null;
        LocalDate nacimiento = rs.getDate("nacimiento").toLocalDate();

        Estudiante estudiante = new Estudiante();
        Centro centro = null;

        // Carga inmediata: obtenemos inmediatamente el centro.
        //if(IdCentro != null) centro = new CentroSqlite(ds).get(IdCentro).orElse(null);

        // Carga perezosa: proxy al que se le carga la clave foránea
        FkLazyLoader<Estudiante> loader = new FkLazyLoader<>(estudiante);
        loader.setFk("centro", idCentro, new CentroSqlite(cp));
        estudiante = loader.createProxy();

        // Cargamos datos en el objeto y entregamos.
        return estudiante.cargarDatos(id, nombre, nacimiento, centro);
    }

    /**
     * Fija los valores de los campos de un registro para una sentencia parametrizada.
     * @param centro El objeto Centro.
     * @param pstmt La sentencia parametrizada.
     * @throws SQLException Cuando se produce un error al establecer valor para los parámentros de la consulta.
     */
    private static void setEstudianteParams(Estudiante estudiante, PreparedStatement pstmt) throws SQLException {
        Centro centro = estudiante.getCentro();

        pstmt.setString(1, estudiante.getNombre());
        pstmt.setDate(2, Date.valueOf(estudiante.getNacimiento()));
        pstmt.setObject(3, centro == null?centro:centro.getId(), Types.INTEGER);
        pstmt.setInt(4, estudiante.getId());
    }

    @Override
    public Stream<Estudiante> get() throws DataAccessException {
        final String sqlString = "SELECT * FROM Estudiante";
        
        try {
            Connection conn = cp.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlString);

            return SqlUtils.resultSetToStream(conn, rs, fila -> resultToEstudiante(fila, cp));
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    @Override
    public Optional<Estudiante> get(int id) throws DataAccessException {
        final String sqlString = "SELECT * FROM Estudiante WHERE id_estudiante = ?";

        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sqlString);
        ) {
            pstmt.setInt(1, id);
            ResultSet rs = pstmt.executeQuery();
            return rs.next()?Optional.of(resultToEstudiante(rs, cp)):Optional.empty();
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    @Override
    public void insert(Estudiante centro) throws DataAccessException {
        final String sqlString = "INSERT INTO Estudiante (nombre, nacimiento, centro, id_estudiante) VALUES (?, ?, ?, ?, ?)";
        
        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sqlString);
        ) {
            setEstudianteParams(centro, pstmt);
            pstmt.executeUpdate();
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    @Override
    public void insert(Iterable<Estudiante> estudiantes) throws DataAccessException {
        final String sqlString = "INSERT INTO Estudiante (nombre, nacimiento, centro, id_estudiante) VALUES (?, ?, ?, ?)";

        try(
            Connection conn = cp.getConnection();
            TransactionManager tm = new TransactionManager(conn);
            PreparedStatement pstmt = tm.getConn().prepareStatement(sqlString);
        ) {
            for(Estudiante estudiante: estudiantes) {
                setEstudianteParams(estudiante, pstmt);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            tm.commit();
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    @Override
    public boolean delete(int id) throws DataAccessException {
        final String sqlString = "DELETE FROM Estudiante WHERE id_estudiante = ?";

        try (
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sqlString);
        ) {
            pstmt.setInt(1, id);
            return pstmt.executeUpdate() > 0;
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    @Override
    public boolean update(Estudiante estudiante) throws DataAccessException {
        final String sqlString = "UPDATE Estudiante SET nombre = ?, nacimiento = ?, centro = ? WHERE id_estudiante = ?";
        
        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sqlString);
        ) {
            setEstudianteParams(estudiante, pstmt);
            return pstmt.executeUpdate() > 0;
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    @Override
    public boolean update(int oldId, int newId) throws DataAccessException {
        final String sqlString = "UPDATE Estudiante SET id_estudiante = ? WHERE id_estudiante = ?";
        
        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sqlString);
        ) {
            pstmt.setInt(1, newId);
            pstmt.setInt(2, oldId);
            return pstmt.executeUpdate() > 0;
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }
}
