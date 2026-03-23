package edu.acceso.sqlutils.backend.mappers;

import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.orm.mapper.Column;
import edu.acceso.sqlutils.orm.mapper.EntityMapper;
import edu.acceso.sqlutils.orm.mapper.TableInfo;

/**
 * DAO para la entidad Estudiante.
 */
public final class EstudianteMapper implements EntityMapper<Estudiante> {

    /** Relación entre la entidad Estudiante y la tabla estudiantes */
    private static final TableInfo TABLE_INFO = new TableInfo(
        "estudiante",
        new Column("id_estudiante", "id"),
        new Column[] {
            new Column("nombre", "nombre"),
            new Column("nacimiento", "nacimiento"),
            new Column("centro", "centro")
        }
    );

    @Override
    public TableInfo getTableInfo() {
        return TABLE_INFO;
    }
}