package annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
public @interface Raw {

    String layer = "raw";

    String fileName() default "";

    String extension() default ".txt";

    String jobName() default "uniformaed";

    String description() default "uniformaed";

    String database() default "uniformaed";

    String tableName() default "uniformaed";

    String layer() default layer;
}
