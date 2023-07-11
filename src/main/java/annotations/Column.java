package annotations;

public @interface Column {
    String columnName() default "";

}
