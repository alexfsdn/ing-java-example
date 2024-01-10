package model.enums;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public enum ProcessAEnum {
    name,
    age,
    cpf,
    dat_ref;

    public static StructType schema() {
        StructType structType = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(ProcessAEnum.name.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProcessAEnum.age.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProcessAEnum.cpf.toString(), DataTypes.StringType, true),
                DataTypes.createStructField(ProcessAEnum.dat_ref.toString(), DataTypes.StringType, true)
        });

        return structType;
    }

}
