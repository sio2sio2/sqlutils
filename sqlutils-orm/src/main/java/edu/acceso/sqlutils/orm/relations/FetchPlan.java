package edu.acceso.sqlutils.orm.relations;

/**
 * Define el plan de carga de entidades relacionadas
 * @param depth Profundidad hasta la cual se hará una carga ansiosa.
 * Si es {@code null}, se sobreentiende una profundidad infinita.
 */
public record FetchPlan(Integer depth) {
   /** Carga ansiosa */
   public static final FetchPlan EAGER = new FetchPlan(null);
   /** Carga perezosa */
   public static final FetchPlan LAZY = new FetchPlan(0);

   @Override
   public String toString() {
      return switch (depth) {
         case null -> "Carga ansionsa (EAGER)";
         case 0 -> "Carga perezosa (LAZY)";
         default -> "Carga con profundidad %d".formatted(depth);
      };
   }
}
