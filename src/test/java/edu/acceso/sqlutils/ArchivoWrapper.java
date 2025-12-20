package edu.acceso.sqlutils;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Envoltorio de archivos para manejar de igual modo
 * la lectura y escritura sobre archivos que sobre la entrada y salida estándar.
 * 
 * <p>Para su uso, hay que crear una instancia facilitando la ruta del archivo:
 * 
 * <pre>ArchivoWrapper archivo = new ArchivoWrapper("/ruta/del/archivo.txt");</pre>
 * 
 * <p>Si se quiere usar un archivo de recursos, hay que anteponer ":resource:" a la ruta:
 * 
 * <pre>ArchivoWrapper archivo = new ArchivoWrapper(":resource:/datos/archivo.txt");</pre>
 * 
 * <p>Si se quiere usar la entrada o salida estándar, hay que crear la instancia
 * pasando null como ruta del archivo:
 * 
 * <pre>ArchivoWrapper archivo = new ArchivoWrapper(null);</pre>
 * 
 * <p>Una vez creada la instancia se usan flujos de entrada o salida con ellos del mismo modo:
 * <pre>try(OutputStream st = archivo.createStream()) {
 *     // Se escribe en el flujo de salida.
 * }
 *
 * try(InputStream st = archivo.openStream()) {
 *     // Se lee del flujo de entrada
 * }</pre>
 * 
 * <p>La propia clase se encarga de no cerrar realmente los flujos estándar al cerrarlos y de
 * provocar un error al intentar escribir en un archivo de recursos.
 */
public class ArchivoWrapper {
    private static final Logger logger = LoggerFactory.getLogger(ArchivoWrapper.class);

    /** Ruta del archivo */
    private Path path;

    /** Ruta asociada a recursos */
    private String resource;

    /**
     * Constructor para envolver un archivo existente.
     * @param ruta Ruta del archivo a envolver. Si null, se usará la entrada/salida estándar.
     */
    public ArchivoWrapper(String ruta) {
        if(ruta == null) return;  // Entrada/salida estándar

        if(ruta.startsWith(":resource:")) {
            this.resource = ruta.substring(":resource:".length());
        }
        else this.path = Path.of(ruta);
    }

    /** Envoltorio para evitar cerrar el flujo subyacente */
    private static class notClosingOutputStream extends FilterOutputStream {
        public notClosingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }

    /** Envoltorio para evitar cerrar el flujo subyacente */
    private static class notClosingInputStream extends FilterInputStream {
        public notClosingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
        }
    }

    /**
     * Verifica si el flujo de salida es el estándar
     * @param st Flujo de salida
     * @return true si es el flujo de salida estándar, false en caso contrario.
     */
    public static boolean isSystemOutput(OutputStream st) {
        return st == System.out || st instanceof notClosingOutputStream;
    }

    /**
     * Verifica si el envoltorio representa un archivo de recursos
     * @return true si el envoltorio representa un archivo de recursos, false en caso contrario
     */
    public boolean isResource() {
        return resource != null;
    }

    /**
     * Verifica si se usa la entrada/salida estándar
     * @return true si se usa la entrada/salida estándar, false en caso contrario.
     */
    public boolean isIOStandart() {
        return path == null && resource == null;
    }

    /**
     * Verifica si el envoltorio representa un archivo físico
     * @return true si el envoltorio representa un archivo físico, false en caso contrario
     */
    public boolean isArchive() {
        return path != null;
    }

    /**
     * Crea un flujo de salida para el archivo
     * @return Flujo de salida para el archivo
     * @throws IOException Si ocurre un error al crear el flujo
     */
    public OutputStream createStream() throws IOException {
        if (path != null) {
            try {
                return Files.newOutputStream(path);
            } catch (IOException e) {
                logger.error("{}: Error al crear el flujo de salida", path);
                throw e;
            }
        }
        else if(resource != null) throw new IOException("No se puede escribir en un recurso");
        else return new notClosingOutputStream(System.out);
    }

    /**
     * Crea un flujo de entrada para el archivo
     * @return Flujo de entrada para el archivo
     * @throws IOException Si ocurre un error al crear el flujo
     */
    public InputStream openStream() throws IOException {
        if (path != null) {
            try {
                return Files.newInputStream(path);
            } catch (IOException e) {
                logger.error("{}: Error al crear el flujo de entrada", path);
                throw e;
            }
        }
        else if(resource != null) {
            InputStream resSt = getClass().getResourceAsStream(resource);
            if(resSt == null) throw new IOException("Recurso inexistente: " + resource);
            return resSt;
        }
        else return new notClosingInputStream(System.in);
    }

    @Override
    public String toString() {
        return path != null
         ? path.toString()
         : resource != null
             ? ":resource:" + resource
             : "E/S estándar";
    }
}
