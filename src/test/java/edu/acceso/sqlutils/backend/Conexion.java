package edu.acceso.sqlutils.backend;

import edu.acceso.sqlutils.dao.Crud;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;

public interface Conexion {

    @FunctionalInterface
    public interface Transaccionable {
        void run(Crud<Centro> centroDao, Crud<Estudiante> estudianteDao) throws DataAccessException;
    }

    public Crud<Centro> getCentroDao();
    public Crud<Estudiante> getEstudianteDao();
    public void transaccion(Transaccionable operaciones) throws DataAccessException;
}
