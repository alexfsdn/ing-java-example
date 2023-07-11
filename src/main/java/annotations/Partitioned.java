package annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Partitioned {

    String partition() default "dt_reference";

    boolean daily() default false;

    boolean weekly() default false;

    boolean monthly() default false;

    boolean eventual() default false;
}

