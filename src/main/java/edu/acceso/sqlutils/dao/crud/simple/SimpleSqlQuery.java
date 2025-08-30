package edu.acceso.sqlutils.dao.crud.simple;
import edu.acceso.sqlutils.dao.crud.MinimalSqlQuery;

/**
 * Consultas SQL necesarias para implementar {@link SimpleCrudInterface}.
 */
public interface SimpleSqlQuery extends MinimalSqlQuery {

    /**
     * Genera el texto de una consulta SQL para seleccionar todos los registros de una tabla.
     * @return La consulta SQL.
     */
    public String getSelectSql();

    /**
     * Genera el texto de una consulta SQL que devuelve los registros uno de cuyos campos es igual a un valor dado.
     * @param column El nombre de la columna.
     * @return La consulta SQL.
     */
    public String getSelectWhereSql(String column);

    /**
     * Genera el texto de una consulta SQL para insertar un nuevo registro en una tabla.
     * @return La consulta SQL.
     */
    public String getInsertSql();

    /**
     * Genera el texto de una consulta SQL para eliminar un registro por su ID.
     * @return La consulta SQL.
     */
    public String getDeleteSql();

    /**
     * Genera el texto de una consulta SQL para actualizar un registro por su ID.
     * @return La consulta SQL.
     */
    public String getUpdateSql();

    /**
     * Genera el texto de una consulta SQL para actualizar el ID de un registro.
     * @return La consulta SQL.
     */
    public String getUpdateIdSql();
}
