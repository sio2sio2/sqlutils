package edu.acceso.sqlutils.dao.relations;

/**
 * Fabrica de cargadores de relaciones.
 * Proporciona cargadores de relaciones inmediatos o perezosos.
 */
public enum LoaderFactory {

    /** Cargador inmediato de relaciones. */
    EAGER(EagerLoader.class),
    /** Cargador perezoso de relaciones. */
    LAZY(LazyLoader.class);

    /** Clase del cargador de relaciones. */
    private Class<? extends RelationLoader> loaderClass;

    /**
     * Constructor de la fábrica de cargadores de relaciones.
     * @param loaderClass Clase del cargador de relaciones.
     */
    LoaderFactory(Class<? extends RelationLoader> loaderClass) {
        this.loaderClass = loaderClass;
    }

    /**
     * Obtiene el cargador de relaciones correspondiente.
     * @return  La clase del cargador de relaciones seleccionado.
     */
    public Class<? extends RelationLoader> getLoaderClass() {
        return loaderClass;
    }
}
