package utils;

import model.Responsavel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.google.common.base.Strings;

public class HierarquiaUtil {

    private static List<String> matriculas;


    public static List<String> hierarquiaBuild(String matricula, Map<String, List<Responsavel>> mapResponsavel) {
        matriculas = new ArrayList<>();

        hierarquia(matricula, mapResponsavel);

        return matriculas;
    }

    private static void hierarquia(String matricula, Map<String, List<Responsavel>> mapResponsavel) {
        List<Responsavel> responsaveis = null;

        if (!Strings.isNullOrEmpty(matricula)) {
            responsaveis = mapResponsavel.get(matricula);
        }

        if (responsaveis != null) {
            if (responsaveis.size() > 0) {
                for (Responsavel resp :
                        responsaveis) {
                    if (!Strings.isNullOrEmpty(resp.getMatriculaResp())) {
                        matriculas.add(resp.getMatriculaResp());
                        hierarquia(resp.getMatriculaResp(), mapResponsavel);
                    }
                }
            }
        }

        return;
    }
}