package edu.acceso.sqlutils.modelo;

import edu.acceso.sqlutils.orm.minimal.Entity;
import jakarta.persistence.Column;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;

/**
 * Modela un centro de enseñanza.
 */
@jakarta.persistence.Entity
public class Centro implements Entity {

    /**
     * Código identificativo del centro.
     */
    @Id
    private Long id;
    /**
     * Nombre del centro.
     */
    @Column(length = 100, nullable = false)
    private String nombre;
    /**
     * Titularidad: pública o privada.
     */
    @Column(nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private Titularidad titularidad;

    public Centro() {
        super();
    }

    /**
     * Carga todos los datos en el objeto.
     * @param id Código del centro.
     * @param nombre Nombre del centro.
     * @param titularidad Titularidad del centro.
     * @return El propio objeto.
     */
    public Centro inicializar(Long id, String nombre, Titularidad titularidad) {
        setId(id);
        setNombre(nombre);
        setTitularidad(titularidad);
        return this;
    }

    /**
     * Constructor que admite todos los datos de definición del centro.
     * @param id Código del centro.
     * @param nombre Nombre del centro.
     * @param titularidad Titularidad del centro (pública o privada)
     */
    public Centro(Long id, String nombre, Titularidad titularidad) {
        inicializar(id, nombre, titularidad);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public Titularidad getTitularidad() {
        return titularidad;
    }

    public void setTitularidad(Titularidad titularidad) {
        this.titularidad = titularidad;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", getNombre(), getId());
    }
}
