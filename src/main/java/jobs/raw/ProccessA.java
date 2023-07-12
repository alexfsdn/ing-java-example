package jobs.raw;

import annotations.Partitioned;
import annotations.Raw;
import interfaces.IProccess;
import model.enums.ProccessAEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;

@Partitioned(daily = true, partition = "dat_ref")
@Raw(fileName = "proccess_a_YYYYMMDD",
        formatDateInTheFileName = "YYYYMMDD",
        extension = "csv",
        jobName = "proccessA",
        description = "teste do processo A",
        database = "databasepro",
        tableName = "tablepro",
        inputHdfs = "src/test/resources/input/",
        outputHdfs = "src/test/resources/output/",
        delimiter = ";",
        header = true
)
public class ProccessA implements IProccess {

    private final String logStarting = String.format("Starting proccess  %s", ProccessA.class);

    private final String INVALID_LINES = "_corrupt_record";
    private final String TIME_STAMP_REFERENCE = "TIME_STAMP_REFERENCE";
    private final String PARTITION_REFERENCE = "PARTITION_REFERENCE";

    @Override
    public void run(SparkSession spark, String dt_ref) {

        System.out.println(logStarting);

        Class<?> class_ = ProccessA.class;
        Raw raw = class_.getAnnotation(Raw.class);

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
        System.out.println("---LIST CONFIG THE END---");

        System.out.println("Period: " + dt_ref);

        StructType schemaA = schema().add(INVALID_LINES, DataTypes.StringType, true);

        String fileName = raw.inputHdfs().concat(raw.fileName().concat(".").concat(raw.extension()).replace(raw.formatDateInTheFileName(), dt_ref));
        Map<String, String> map = new HashMap<>();
        map.put("delimiter", raw.delimiter());
        map.put("header", String.valueOf(raw.header()));
        map.put("encoding", raw.encoding());

        System.out.println("FILE_NAME: " + fileName);
        Dataset<Row> dataset = spark.read()
                .format(raw.extension())
                .options(map)
                .schema(schemaA)
                .option("mode", "PERMISSIVE")
                .load(fileName).cache();

        dataset = dataset.filter(col(INVALID_LINES).isNull())
                .drop(col(INVALID_LINES));

        dataset = dataset.withColumn(TIME_STAMP_REFERENCE, current_timestamp())
                .withColumn(PARTITION_REFERENCE, lit(dt_ref)).select(col(ProccessAEnum.name.name()),
                        col(ProccessAEnum.age.name()),
                        col(ProccessAEnum.cpf.name()),
                        col(ProccessAEnum.dat_ref.name()),
                        col(TIME_STAMP_REFERENCE),
                        col(PARTITION_REFERENCE)).persist(StorageLevel.MEMORY_ONLY());

        dataset.write().format("hive").mode(SaveMode.Append)
                .option("partitionOverwriteMode", "dynamic")
                .insertInto("testDB.tableUser");

    }


    public StructType schema() {
        StructType structType = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(ProccessAEnum.name.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProccessAEnum.age.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProccessAEnum.cpf.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProccessAEnum.dat_ref.toString(), DataTypes.StringType, true)
        });

        return structType;
    }
}
