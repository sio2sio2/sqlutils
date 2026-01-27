package edu.acceso.sqlutils.modelo;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import edu.acceso.sqlutils.crud.Entity;

/**
 * Modela un estudiante.
 */
public class Estudiante implements Entity {

    /**
     * Identificador del estudiante.
     */
    private Long id;
    /**
     * Nombre completo del estudiante.
     */
    private String nombre;
    /**
     * Fecha de nacimiento del estudiante.
     */
    private LocalDate nacimiento;

    /**
     * Centro al que está adscrito.
     */
    private Centro centro;

    public Estudiante() {
        super();
    }

    /**
     * Carga los datos del estudiante.
     * @param id El identificador del estudiante.
     * @param nombre El nombre del estudiante.
     * @param nacimiento La fecha de nacimiento.
     * @param centro El centro al que está adscrito.
     * @return El propio objeto.
     */
    public Estudiante cargarDatos(Long id, String nombre, LocalDate nacimiento, Centro centro) {
        setId(id);
        setNombre(nombre);
        setNacimiento(nacimiento);
        setCentro(centro);

        return this;
    }

    /**
     * Constructor que carga todos los datos.
     * @param id El identificador del estudiante.
     * @param nombre El nombre del estudiante.
     * @param nacimiento La fecha de nacimiento.
     * @param centro El centro al que está adscrito.
     * @return El propio objeto.
     */
    public Estudiante(Long id, String nombre, LocalDate nacimiento, Centro centro) {
        this.cargarDatos(id, nombre, nacimiento, centro);
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
    public LocalDate getNacimiento() {
        return nacimiento;
    }
    public void setNacimiento(LocalDate nacimiento) {
        this.nacimiento = nacimiento;
    }
    public Centro getCentro() {
        return centro;
    }
    public void setCentro(Centro centro) {
        this.centro = centro;
    }
    
    @Override
    public String toString() {
        LocalDate hoy = LocalDate.now();
        String nombreCentro = getCentro() == null ? "N/A" : getCentro().getNombre();

        return String.format("%s (%d años, %s)", getNombre(), ChronoUnit.YEARS.between(getNacimiento(), hoy), nombreCentro);
    }
}
