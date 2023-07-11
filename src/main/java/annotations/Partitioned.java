package annotations;

public @interface Partitioned {

    String partition() default "dt_reference";

    boolean daily() default false;

    boolean weekly() default false;

    boolean monthly() default false;

    boolean eventual() default false;
}

