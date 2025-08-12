package edu.acceso.sqlutils.modelo;

import java.util.Arrays;

public enum Titularidad {

    PUBLICA("pública"),
    PRIVADA("privada");

    private String desc;

    Titularidad(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return desc;
    }

    public static Titularidad fromString(String desc) {
        return Arrays.stream(Titularidad.values())
                .filter(t -> t.desc.equalsIgnoreCase(desc))
                .findFirst()
                .orElse(null);
    }
}
