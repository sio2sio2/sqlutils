package edu.acceso.sqlutils.dao.mapper;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Clase para traducir tipos de datos básicos entre Java y SQL.
 */
public class SqlTypesTranslator {

    /**
     * Tipo de dato SQL.
     */
    private final int type;
    /**
     * Valor del dato SQL.
     */
    private Object value;

    public SqlTypesTranslator(Class<?> type, Object value) {
        this.type = getSqlType(type);
        setSqlValue(value);
    }

    /**
     * Obtiene el tipo de dato SQL correspondiente a una clase Java.
     * @param type La clase Java.
     * @return El tipo de dato SQL correspondiente.
     */
    private static int getSqlType(Class<?> type) {
        return switch (type.getName()) {
            case "java.lang.String" -> Types.VARCHAR;
            case "java.lang.Integer", "int" -> Types.INTEGER;
            case "java.lang.Long", "long" -> Types.BIGINT;
            case "java.lang.Double", "double" -> Types.DOUBLE;
            case "java.lang.Float", "float" -> Types.FLOAT;
            case "java.lang.Boolean", "boolean" -> Types.BOOLEAN;
            case "java.time.LocalDate" -> Types.DATE;
            case "java.time.LocalTime" -> Types.TIME;
            case "java.time.LocalDateTime" -> Types.TIMESTAMP;
            case "java.math.BigDecimal" -> Types.DECIMAL;
            default -> throw new IllegalArgumentException("Tipo no soportado: " + type.getName());
        };
    }
    
    /**
     * Establece el valor SQL correspondiente al valor Java proporcionado.
     * @param value El valor Java.
     * @return El valor SQL correspondiente.
     */
    private void setSqlValue(Object value) {
        this.value = value == null 
        ? null
        : switch(type) {
            case Types.DATE -> Date.valueOf((LocalDate) value);
            case Types.TIME -> Time.valueOf((LocalTime) value);
            case Types.TIMESTAMP -> Timestamp.valueOf((LocalDateTime) value);
            default -> value;
        };
    }

    /**
     * Obtiene el tipo de dato SQL.
     * @return El tipo de dato SQL.
     */
    public int getType() {
        return type;
    }

    /**
     * Obtiene el valor SQL.
     * @return El valor SQL.
     */
    public Object getSqlValue() {
        return value;
    }
}
