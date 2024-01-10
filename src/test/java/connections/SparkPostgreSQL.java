package connections;

import org.apache.spark.sql.*;

import java.util.Properties;

public class SparkPostgreSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkPostgreSQLExample")
                .master("local[*]") // Configuração do master, pode variar
                .getOrCreate();

        // Configurações para conectar ao PostgreSQL
        String url = "jdbc:postgresql://localhost:5432/andromeda";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        props.setProperty("driver", "org.postgresql.Driver");

        // Carregar dados da tabela do PostgreSQL
        Dataset<Row> data = spark.read().jdbc(url, "spark.usuario", props);

        // Mostrar os dados carregados
        data.show();

        spark.stop();
    }
}
