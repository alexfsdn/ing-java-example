package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import enums.DelimitadorEnum;
import model.enums.ResponsavelEnum;
import org.apache.commons.lang3.StringUtils;

public class Responsavel {
    private String matricula;
    private String matriculaResp;

    public Responsavel() {

    }

    public Responsavel(String matricula, String matriculaResp) {

        this.matricula = matricula;
        this.matriculaResp = matriculaResp;
    }

    public static Responsavel parseFromText(String text) {

        if (!StringUtils.isEmpty(text.toString())) {

            String[] line = text.toString().split(DelimitadorEnum.PONTO_E_VIRGULA.name(), -1);

            if (line.length > 0) {
                String matricula = line[ResponsavelEnum.matricula.ordinal()].trim();
                String matriculaResponsavel = line[ResponsavelEnum.matricula_resp.ordinal()].trim();
                return new Responsavel(matricula, matriculaResponsavel);
            }

        }
        return null;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.matricula);
        out.writeUTF(this.matriculaResp);
    }

    public void readFields(DataInput in) throws IOException {
        this.matricula = in.readUTF();
        this.matriculaResp = in.readUTF();
    }


    public void clean() {
        this.matricula = StringUtils.EMPTY;
        this.matriculaResp = StringUtils.EMPTY;
    }

    public String getMatricula() {
        return matricula;
    }

    public void setMatricula(String matricula) {
        this.matricula = matricula;
    }

    public String getMatriculaResp() {
        return matriculaResp;
    }

    public void setMatriculaResp(String matriculaResp) {
        this.matriculaResp = matriculaResp;
    }

}
