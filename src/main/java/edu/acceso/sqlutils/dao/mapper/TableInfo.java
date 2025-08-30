package edu.acceso.sqlutils.dao.mapper;

import java.util.Arrays;

/**
 * Clase que representa la información de una tabla en la base de datos.
 * @param tableName Nombre de la tabla.
 * @param idColumn Columna que representa la clave primaria.
 * @param columns Columnas de la tabla.
 */
public record TableInfo( String tableName, Column idColumn, Column[] columns) {
    /** 
     * Devuelve los nombres de las columnas que no son clave primaria
     * @return Array de nombres de columnas.
     */
    public String[] getColumnNames() {
        return Arrays.stream(columns)
                .map(Column::getName)
                .toArray(String[]::new);
    }

    /**
     * Devuelve los nombres de los campos de la entidad (excepto la clave primaria).
     * @return Array de nombres de campos.
     */
    public String[] getFieldNames() {
        return Arrays.stream(columns)
                .map(Column::getField)
                .toArray(String[]::new);
    }

    /**
     * Verifica si un campo existe en la entidad.
     * @param fieldName Nombre del campo a verificar.
     * @return true si el campo existe, false en caso contrario.
     */
    public boolean fieldExists(String fieldName) {
        return fieldName.equalsIgnoreCase(idColumn.getField()) || Arrays.stream(columns)
                .anyMatch(column -> column.getField().equalsIgnoreCase(fieldName));
    }

    /**
     * Verifica que un campo sea la clave primaria de la entidad.
     * @param fieldName Nombre del campo a verificar.
     * @return true si el campo es la clave primaria, false en caso contrario.
     */
    public boolean isIdColumn(String fieldName) {
        return idColumn.getField().equalsIgnoreCase(fieldName);
    }

    /**
     * Obtiene, dado su nombre. el tipo de dato de un campo
     * @param fieldName Nombre del campo.
     * @return Clase del tipo de dato del campo.
     */
    public Class<?> getFieldType(String fieldName) {
        if (isIdColumn(fieldName)) return Long.class;
        return Arrays.stream(columns)
                .filter(column -> column.getField().equalsIgnoreCase(fieldName))
                .findFirst()
                //.map(Column::fieldType)  // <-- ¿Por qué esto da error?
                //.orElseThrow(() -> new IllegalArgumentException("Campo no encontrado: " + fieldName));
                .orElseThrow(() -> new IllegalArgumentException("Campo no encontrado: " + fieldName))
                .getFieldType();
    }

    /**
     * Obtiene el nombre de la columna SQL correspondiente a un campo.
     * @param fieldName Nombre del campo.
     * @return Nombre de la columna SQL.
     */
    public String getColumnName(String fieldName) {
        if (isIdColumn(fieldName)) return idColumn.getName();
        return Arrays.stream(columns)
                .filter(column -> column.getField().equalsIgnoreCase(fieldName))
                .findFirst()
                .map(Column::getName)
                .orElseThrow(() -> new IllegalArgumentException("Campo no encontrado: " + fieldName));
    }
}