package edu.acceso.sqlutils.processor;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import edu.acceso.sqlutils.annotations.Fk;

/**
 * Verifica durante la compilación que todos los campos definidos
 * como clave foránea tienen su getter correspondiente.
 */
@SupportedAnnotationTypes("edu.acceso.sqlutils.annotations.Fk")
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class FkAnnotationProcessor extends AbstractProcessor {

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        // Para todas las claves foráneas.
        for(Element element : roundEnv.getElementsAnnotatedWith(Fk.class)) {
            if (element.getKind() != ElementKind.FIELD) {
                error(element, "La anotación @Fk solo es aplicable a campos.");
                continue;
            }

            // Verifica que el campo tiene un getter
            TypeElement classElement = (TypeElement) element.getEnclosingElement();
            String fieldName = element.getSimpleName().toString();
            String getterName = "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

            boolean hasGetter = classElement.getEnclosedElements().stream()
                .filter(e -> e.getKind() == ElementKind.METHOD)
                .map(Element::getSimpleName)
                .anyMatch(name -> name.contentEquals(getterName));

            if (!hasGetter) {
                error(element, String.format("El campo '%s' anotado con @Fk debe tener un getter llamado '%s'.", fieldName, getterName));
            }
        }
        return true;
    }

    private void error(Element e, String msg) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, msg, e);
    }
}
