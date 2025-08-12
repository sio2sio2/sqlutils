package edu.acceso.sqlutils.dao.query;

import java.util.Collections;

/** Clase que genera consultas SQL para operaciones CRUD básicas. */
public class SqlQuery {

    private final String tableName;
    private final String idColumn;
    private final String[] columns;

    /**
     * Constructor de la clase SqlQuery.
     * @param tableName El nombre de la tabla.
     * @param idColumn El nombre de la columna que representa el ID.
     * @param columns Los nombres de las columnas a actualizar.
     */
    public SqlQuery(String tableName, String idColumn, String ... columns) {
        this.tableName = tableName;
        this.idColumn = idColumn;
        this.columns = columns;
    }

    /**
     * Genera una consulta SQL para seleccionar todos los registros de una tabla.
     * @return La consulta SQL.
     */
    public String getSelectSql() {
        return String.format("SELECT * FROM %s", tableName);
    }

    /**
     * Genera una consulta SQL para seleccionar un registro por su ID.
     * @return La consulta SQL.
     */
    public String getSelectIdSql() {
        return getSelectWhereSql(idColumn);
    }

    /**
     * Genera una consulta SQL que devuelve los registros uno de cuyos campos es igual a un valor dado.
     * @return La consulta SQL.
     */
    public String getSelectWhereSql(String column) {
        return String.format("SELECT * FROM %s WHERE %s = ?", tableName, column);
    }

    /**
     * Genera una consulta SQL para insertar un nuevo registro en una tabla.
     * @return La consulta SQL.
     */
    public String getInsertSql() {
        String columnList = String.join(", ", columns);
        String valuePlaceholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        return String.format("INSERT INTO %s (%s, %s) VALUES (?, %s)", tableName, columnList, idColumn, valuePlaceholders);
    }

    /**
     * Genera una consulta SQL para eliminar un registro por su ID.
     * @return La consulta SQL.
     */
    public String getDeleteSql() {
        return String.format("DELETE FROM %s WHERE %s = ?", tableName, idColumn);
    }

    /**
     * Genera una consulta SQL para actualizar un registro por su ID.
     * @return La consulta SQL.
     */
    public String getUpdateSql() {
        String setClause = String.join(" = ?, ", columns) + " = ?";
        return String.format("UPDATE %s SET %s WHERE %s = ?", tableName, setClause, idColumn);
    }

    /**
     * Genera una consulta SQL para actualizar el ID de un registro.
     * @return La consulta SQL.
     */
    public String getUpdateIdSql() {
        return String.format("UPDATE %s SET %s = ? WHERE %s = ?", tableName, idColumn, idColumn);
    }
}
