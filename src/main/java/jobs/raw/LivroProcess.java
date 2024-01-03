package jobs.raw;

import annotations.Partitioned;
import annotations.Raw;
import dtos.LivroDto;
import interfaces.IProcess;
import model.enums.ProcessAEnum;
import model.enums.LivroEnum;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import utils.TodayUtils;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;

@Partitioned(weekly = true, daily = false, monthly = false, partition = "dat_ref")
@Raw(fileName = "livros_YYYYMMDD",
        formatDateInTheFileName = "YYYYMMDD",
        extension = "csv",
        jobName = "livrosProcess",
        description = "teste do livrosProcess",
        database = "livroDB",
        tableName = "livrosTable",
        inputHdfs = "src/test/resources/input/",
        outputHdfs = "src/test/resources/output/",
        delimiter = ";",
        header = true
)
public class LivroProcess implements IProcess {

    private final String logStarting = String.format("Starting process  %s", LivroProcess.class);
    private final String TIME_STAMP_REFERENCE = "TIME_STAMP_REFERENCE";
    private RestTemplate restTemplate;
    final java.net.URI URI = new URI("http://localhost:8080/livros");
    public LivroProcess() throws URISyntaxException {
    }

    @Override
    public void run(SparkSession spark, String dt_ref) {

        System.out.println(logStarting);

        Class<?> class_ = LivroProcess.class;
        Raw raw = class_.getAnnotation(Raw.class);
        Partitioned partitioned = class_.getAnnotation(Partitioned.class);

        System.out.println("---LIST CONFIG---");
        System.out.println("FileName: " + raw.fileName());
        System.out.println("Extesion: " + raw.extension());
        System.out.println("JobName: " + raw.jobName());
        System.out.println("Description: " + raw.description());
        System.out.println("Database: " + raw.database());
        System.out.println("TableName: " + raw.tableName());
        System.out.println("Delimiter: " + raw.delimiter());
        System.out.println("Input hdfs: " + raw.inputHdfs());
        System.out.println("Output hdfs: " + raw.outputHdfs());
        System.out.println("Layer: " + raw.layer());
        System.out.println("Header: " + raw.header());
        System.out.println("encoding: " + raw.encoding());
        System.out.println("Pationed: " + partitioned.partition());
        System.out.println("Period daily: " + partitioned.daily());
        System.out.println("Period weekly: " + partitioned.weekly());
        System.out.println("Period mothly: " + partitioned.monthly());
        System.out.println("Period eventual: " + partitioned.eventual());
        System.out.println("---LIST CONFIG THE END---");

        System.out.println("Period: " + dt_ref);

        final ParameterizedTypeReference<List<LivroDto>> TYPE_LIST_OF_LIVRO_VO = new ParameterizedTypeReference<List<LivroDto>>() {
        };

        final HttpEntity<LivroDto> LIVRO_NULL = null;

        restTemplate = new RestTemplate();
        ResponseEntity<List<LivroDto>> result = restTemplate.exchange(URI, HttpMethod.GET,
                LIVRO_NULL, TYPE_LIST_OF_LIVRO_VO);

        List<LivroDto> livros = result.getBody();

        for (LivroDto dto :
                livros) {

            System.out.println("ID: " + dto.getId());
            System.out.println("Nome: " + dto.getNome());
            System.out.println("Nome da Editora: " + dto.getNomeDaEditora());
            System.out.println("Número de páginas: " + dto.getNumeroPaginas());
        }

        Dataset<Row> livrosDataset = spark.createDataFrame(livros, LivroDto.class);

        String tableNameTMP = raw.tableName().concat(TodayUtils.getTodayOnlyNumbers());

        livrosDataset.withColumn(TIME_STAMP_REFERENCE, current_timestamp())
                .select(col(LivroEnum.id.name()),
                        col(LivroEnum.nome.name()),
                        col(LivroEnum.capaDura.name()),
                        col(LivroEnum.numeroPaginas.name()),
                        col(LivroEnum.nomeDaEditora.name()),
                        col(TIME_STAMP_REFERENCE)).createOrReplaceTempView(tableNameTMP);

        String tableName = raw.database().concat(".").concat(raw.tableName());

        String query = "INSERT OVERWRITE TABLE " + tableName + " PARTITION (" + partitioned.partition() + "=" + dt_ref + ") " +
                "SELECT " +
                LivroEnum.id.name() + ", " +
                LivroEnum.nome.name() + ", " +
                LivroEnum.capaDura.name() + ", " +
                LivroEnum.numeroPaginas.name() + ", " +
                LivroEnum.nomeDaEditora.name() + ", " +
                TIME_STAMP_REFERENCE + " FROM " + tableNameTMP;

        System.out.println(query);

        spark.sqlContext().sql(query);

    }


    public StructType schema() {
        StructType structType = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(ProcessAEnum.name.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProcessAEnum.age.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProcessAEnum.cpf.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProcessAEnum.dat_ref.toString(), DataTypes.StringType, true)
        });

        return structType;
    }
}
