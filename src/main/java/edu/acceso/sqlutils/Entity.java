package edu.acceso.sqlutils;

/**
 * Interfaz que obliga a que todos
 * los objetos tengan un identificador numérico.
 */
public interface Entity {
    public int getId();
    public void setId(int id);
}
