package edu.acceso.sqlutils.backend.mappers;

import edu.acceso.sqlutils.dao.mapper.Column;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.mapper.TableInfo;
import edu.acceso.sqlutils.dao.mapper.Translator;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Titularidad;

/**
 * DAO para la entidad Centro.
 */
public final class CentroMapper implements EntityMapper<Centro> {

    /** Relación entre la entidad Centro y la tabla centros */
    private static final TableInfo TABLE_INFO = new TableInfo(
        "centro",
        new Column("id_centro", "id"),
        new Column[] {
            // No es necesario especificar el tipo porque se infiere automáticamente
            // pero se puede hacer si se desea forzar por algún motivo.
            new Column("nombre", "nombre"),
            // Para el Enum necesitamos un traductor
            new Column("titularidad", "titularidad", new Translator() {
                @Override
                public Object serialize(Object value) {
                    return value != null ? value.toString() : null;
                }

                @Override
                public Object deserialize(Object value) {
                    return value != null ? Titularidad.fromString(value.toString()) : null;
                }
            })
        }
    );

    @Override
    public TableInfo getTableInfo() {
        return TABLE_INFO;
    }
}