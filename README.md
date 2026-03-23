## Sqlutils

Sqlutils es una librería de interés meramente pedagógico para complementar [los apuntes el módulo de *Acceso a datos*](https://github.io/sio2sio2/acceso-datos)
del ciclo formativo de grado superior de *Desarrollo de Aplicaciones Multiplataforma*. Consta fundamentalmente de las siguientes partes:

### Clase SqlUtils
Es una clase que con métodos estáticos que pueden ser puntualmente útiles:

`Stream<ResultSet> SqlUtils.resultSetToStream(Connection, Statement, ResultSet)`  
Convierte los resultados de una consulta que devuelve varias filas en un `Stream` de Java. Al cerrar el flujo, se cierra la conexión:

```java
try (
    Connection conn = ds.getConnection();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM Centro");
) {
    SqlUtils.resultSetToStream(conn, stmt, rs).forEach(result -> {
        // En cada iteración result incluye el ResultSet correspondiente
    });
}
```

El método necesita el objeto `Connection` y el `Statement` para poder cerrarlos al acabar. De ese modo, no se requerirá que los cerremos explícitamente.
Si por el contrario, nuestra intención fuera que los objetos no se cerraran, podríamos pasar `null` en vez de los propios objetos.

`<T> Stream<T> SqlUtils.resultSetToStream(Connection, Statement, ResultSet, CheckedFunction<ResultSet, T>)`  
Hace la misma tarea que el método anterior, pero permite incluir una expresión lambda que toma el `ResultSet` de cada fila y lo convierte en un objeto `T`. La expresión lambda puede generar `SQLException`:

```java
/**
 * Genera un objeto Centro a partir de los datos de una fila
 * @param rs Los datos de la fila.
 * @return El objeto Centro generado
 * @throws SQLException Si se produce algún error con la base de datos al obtener los datos de la fila.
 */
private static Centro resultToCentro(ResultSet rs) throws SQLException {
    int id = rs.getInt("id");
    String nombre = rs.getString("nombre");

    return new Centro(id, nombre);
}

/**
 * Obtiene todos los centros de su tabla correspondiente.
 * @return Un flujo con los objetos Centro obtenidos.
 * @throws DataAccessRuntimeException Si se produce algún error al obtener los centros.
 */
public Stream<Centro> obtenerCentros() {
    Connection conn = getConnection();  // Obtenemos una conexión a saber cómo
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM Centro");

    return SqlUtils.resultSetToStream(conn, stmt, rs, EstaClase::resultToCentro);
}
```

El flujo necesita que la conexión no se cierre, de ahí que se haya dejado abierta. Por eso motivo, el propio flujo se encarga de cerrarla, cuando él mismo se cierra:

```java
try(Stream<Centro> centros = consultas.obtenerCentros()) {
    // Operamos con el flujo.
}
```

`boolean isDatabaseEmpty(Connection conn)`  
Comprueba si una base de datos tiene definido su esquema:

```java
boolean empty = SqlUtils.isDatabaseEmpty(conn);
if(empty) {
    // La base de datos debería inicializarse.
}
```

`List<String> splitSQL(InputStream) throws SQLException`  
Descompone un guión SQL estándar en sentencias individuales. No atiende a SQL que no sea estándar.

```java
String script = "guion.sql";
try(String sqlScript = Files.readString(script, StandardCharsets.UTF_8)) {
    List<String> sentencias = splitSQL(sqlScript);

    // Ejecutamos las sentencias
}
```

`void executeSQL(Connection, InputStream) throws SQLException, IOException`  
Ejecuta un guión SQL haciendo uso del método anterior para descomponer en sentencias simples.

### Clase TransactionManager
Implementa un gestor para el manejo de transacciones que libera al desarrollador de la responsabilidad de abrir y cerrar transacciones y conexiones. Utiliza una patrón [Multiton](https://en.wikipedia.org/wiki/Multiton_pattern) y una clave para identificar a cada una de las instancias:

```java
TransactionManager tm = TransactionManager.create("BD", ds);  // ds es un DataSource ya creado.

// Ordenamos operaciones dentro de la transacción mediante una expresión lambda
// ctxt es un contexto que proporciona datos relacionados con la transacción.
tm.transaction(ctxt -> {
    Connection conn = ctxt.connection();  // Conexión protegida frente a cierres manuales.

    // ... Operamos con la base de datos ...
});
```
Soporta transacciones anidadas, pero el efecto de iniciar una transacción dentro de otra es, simplemente aumentar un contador interno (consultable a través del contexto): las operaciones se confirman o rechazan sólo al culminar la transacción raíz. 

El gestor, además, permite la definición y registro de *observadores* que pueden reaccionar ante los siguientes eventos:

- **onBegin**: Cuando una transacción comienza.
- **onCommit**: Cuando las operaciones de una transacción se confirman.
- **onRollBack**: Cuando las operaciones de una transacción se rechazan.
- **onTransactionStart**: Cuando una transacción anidada comienza.
- **onTransactionEnd**: Cuando una transacción anidada culmina.

La definición de un *observador* es sencilla:

```java
/**
 Observador que se limita a definir un contador de operaciones
 y mostrar el resultado al completarse la transacción.
 */
public class CounterListener extends ContextAwareEventListener {
    // Clave para poder obtener luego el observador.
    public static final String KEY = new Object().toString();

    // Recurso que queremos manejar con este observador (el propio contador)
    @Override
    public Object createResource() {
        return new AtomicInteger(0);
    }

    /**
     * Operación que incrementa el contador: cada vez que operemos dentro
     * de la transacción debemos usar este método para llevar la cuenta.
     * @return La cantidad de operaciones hechas hasta el momento.
     */
    public int increment() {
        AtomicInteger counter = getContext().getResource();
        return counter.incrementAndGet();
    }

    @Override
    public void onCommit() {
        AtomicInteger counter = getContext().getResource();
        System.out.printf("Se han realizado %d operaciones dentro de esta transacción.\n", counter.get());
    }

    @Override
    public void onRollback() {
        AtomicInteger counter = getContext().getResource();
        System.out.printf("Se intentaron completar sin llegarse a confirmar %d operaciones dentro de esta transacción.\n", counter.get());
    }

    // Para los restantes eventos no es necesario hacer nada.

}
```

Este *observador* no es excesivamente útil, pero sirve para ilustrar cómo crear uno que maneje un recurso propio (el contador). La librería viene
con uno ya definido llamado `LoggingManager`, que sirve para diferir el registro
de mensajes hasta que se conozca el resultado de la transacción:


```java
TransactionManager tm = TransactionManager.create("BD", ds)
    .addEventListener(CounterListener.KEY, new CounterListener()) // Registramos el observador.
    .addEventListener(LoggingManager.KEY, new LoggingManager());  // Otro observador

tm.transaction(ctxt -> {
    Connection conn = ctxt.connection();  // Conexión protegida frente a cierres manuales.
    LoggingManager logger = ctxt.getEventListener(LoggingManager.KEY, LoggingManager.class);
    CounterListener counter = ctxt.getEventListener(CounterListener.KEY, CounterListener.class);

    // ... Realizamos una operación de inserción cualquiera (p.ej. una inserción) ...

    // Una vez hecha
    logger.sendMessage(
        getClass(),
        Level.DEBUG,                                    // Nivel de gravedad.
        "Inserción del objeto",                         // Mensaje cuando la transcción se confirma.
        "Transacción fallida: no se agrega el objeto"   // Mensaje cuando la transacción se desecha.
    );
    counter.increment();
    
    // Más operaciones.
});
```

### Clase ConnectionPool
La otra gran clase de la librería es `ConnectionPool`, diseñada para definir de
forma sencilla un *pool* de conexiones con
[HikariCP](https://github.com/brettwooldridge/HikariCP):

```java
try(ConnectionPool cp = ConnectionPool.create("DB", dbUrl, dbUser, dbPassword)) {
    DataSource ds = cp.getDataSource();
    try(Connection conn = ds.getConnection()) {

        // ... Operamos con la conexión ...

    }
}
```

La clase, como la anterior, implementa el patrón *Multiton* y usa una clave para
identificar las distintas instancias. Aunque puede usarse de forma
independiente, tal como se ha ilustrado, es más conveniente hacerla funcionar en
conjunción con el gestor de transacciones:

```java
try(ConnectionPool cp = ConnectionPool.create("DB", dbUrl, dbUser, dbPassword)) {
    // Creamos el gestor asociado con los listener que queramos
    TransactionManager tm = cp.initTransactionManager(
        LoggingManager.KEY, new LoggingManager(),
        CounterListener.KEY, new CounterListener(),
    );

    // Ahora podemos operar usando este gestor
    tm.transaction(ctxt -> {

    // ... Operaciones ...

    });

    // El gestor se puede obtener en cualquier momento
    tm = cp.getTransactionManager();
}
```
