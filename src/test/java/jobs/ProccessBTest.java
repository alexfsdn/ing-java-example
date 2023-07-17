package jobs;

import base.services.SparkSessionsServices;
import model.enums.LivroEnum;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;

import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;

@Configuration
@EnableAutoConfiguration
public class ProccessBTest {

    private SparkSession spark = SparkSessionsServices.devLocalEnableHiveSupport();

    private RestTemplate restTemplate;

    final URI URI = new URI("http://localhost:8080/livros");

    public ProccessBTest() throws URISyntaxException {
    }

    private final String TIME_STAMP_REFERENCE = "TIME_STAMP_REFERENCE";
    private final String PARTITION_REFERENCE = "dat_ref";

    public void buildMock() {
        SQLContext hiveContext = spark.sqlContext();

        hiveContext.sql("CREATE DATABASE IF NOT EXISTS livroDB");

        hiveContext.sql("CREATE TABLE IF NOT EXISTS livroDB.livrosTable ( " +
                LivroEnum.id.name() + " STRING, " +
                LivroEnum.nome.name() + " STRING, " +
                LivroEnum.capaDura.name() + " STRING, " +
                LivroEnum.numeroPaginas.name() + " STRING, " +
                LivroEnum.nomeDaEditora.name() + " STRING, " +
                TIME_STAMP_REFERENCE + " TIMESTAMP )" +
                "PARTITIONED BY (" + PARTITION_REFERENCE + " STRING)");

    }

    public void cleanUp() {
        spark.sqlContext().sql("DROP DATABASE IF EXISTS livroDB CASCADE");
    }


    @Test
    public void test() throws RestClientException, URISyntaxException {
        cleanUp();
        buildMock();

        String label_20230711 = "20230711";
        String args[] = {"ProccessB", label_20230711};
        JobRun.run(args, spark);

        spark.sqlContext().sql("select * from livroDB.livrosTable").show(20, false);

        cleanUp();
    }
}
