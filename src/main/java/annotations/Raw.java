package annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Raw {

    String layer = "raw";

    String fileName() default "";

    String formatDateInTheFileName() default "YYYYMMDD";

    String extension() default "txt";

    String jobName() default "uniformaed";

    String description() default "uniformaed";

    String database() default "uniformaed";

    String tableName() default "uniformaed";

    String delimiter() default ";";

    String inputHdfs() default "/data/input";

    String outputHdfs() default "/data/ouput/";

    String layer() default layer;

    boolean header() default false;

    String encoding() default "UTF-8";
}
