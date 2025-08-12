package edu.acceso.sqlutils.dao.mapper;

import edu.acceso.sqlutils.crud.Entity;

/**
 *  Representa una columna de una tabla en la base de datos.
 *  Incluye información sobre el nombre de la columna, el campo asociado en la entidad
 *  y el tipo de dato. Si el tipo es nulo, se usa reflexión para determinarlo.
 */
public class Column {

    private String name;
    private String field;
    private Class<?> fieldType;
    private Translator translator;

    public Column(String name, String field, Class<?> fieldType) {
        this.name = name;
        this.field = field;
        this.fieldType = fieldType;
        this.translator = null;
    }

    /**
     * Constructor alternativo que deja a {@code null} el tipo de dato.
     * En este caso, se usará reflexión para determinarlo.
     * @param name Nombre de la columna
     * @param field Campo asociado en la entidad
     */
    public Column(String name, String field) {
        this(name, field, (Class<?>) null);
    }

    public Column(String name, String field, Translator translator) {
        this(name, field, (Class<?>) null);
        this.translator = translator;
    }

    /**
     * Comprueba si el tipo de dato es una clave foránea. 
     * 
     * @param fieldType Tipo de dato a comprobar
     * @return {@code true} si el tipo es una entidad, {@code false} en caso contrario
     */
    public static boolean isForeignKey(Class<?> fieldType) {
        return Entity.class.isAssignableFrom(fieldType);
    }

    public String getName() {
        return name;
    }

    public String getField() {
        return field;
    }

    public Class<?> getFieldType() {
        return fieldType;
    }

    public Translator getTranslator() {
        return translator;
    }
}