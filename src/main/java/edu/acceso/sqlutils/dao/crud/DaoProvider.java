package edu.acceso.sqlutils.dao.crud;

/**
 * Proveedor de DAOs que proporciona una implementación de las operaciones CRUD y una definición de las consultas SQL.
 * Permite configurar qué clase se usará para definir las operaciones CRUD. En previsión de que un SGBD obligue a definir
 * de distinto modo una determinada sentencia SQL las cadenas que definen las sentencias SQL se independizan en una clase
 * separada.
 */
public class DaoProvider {

    /** Clase que implementa las operaciones CRUD */
    @SuppressWarnings("rawtypes")
    private final Class<? extends AbstractCrud> crudClass;
    /** Clase que implementa las sentencias SQL */
    private final Class<? extends MinimalSqlQuery> sqlQueryClass;

    /**
     * Constructor para el proveedor de DAOs.
     * @param crudClass Clase que implementa las operaciones CRUD.
     * @param sqlQueryClass Clase que implementa las sentencias SQL para dichas operaciones
     */
    public DaoProvider(@SuppressWarnings("rawtypes") Class<? extends AbstractCrud> crudClass,
                       Class<? extends MinimalSqlQuery> sqlQueryClass) {
        this.crudClass = crudClass;
        this.sqlQueryClass = sqlQueryClass;
    }

    /**
     * Obtiene la clase que implementa las operaciones CRUD.
     * @return Clase que implementa las operaciones CRUD
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends AbstractCrud> getCrudClass() {
        return crudClass;
    }

    /**
     * Obtiene la clase que implementa las sentencias SQL.
     * @return Clase que implementa las sentencias SQL
     */
    public Class<? extends MinimalSqlQuery> getSqlQueryClass() {
        return sqlQueryClass;
    }
}
