package utils;

import com.google.common.base.Strings;
import model.Responsavel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UsuarioHierarquiaUtil {

    private static List<List<String>> matriculasResponsaveis;
    private static List<String> matriculas;

    public static List<List<String>> hierarquiaBuild(String matricula, Map<String, List<Responsavel>> mapResponsavel) {
        matriculas = new ArrayList<>();
        matriculasResponsaveis = new ArrayList<List<String>>();

        hierarquia(matricula, mapResponsavel);

        return matriculasResponsaveis;
    }

    public static void hierarquia(String matricula, Map<String, List<Responsavel>> mapResponsavel) {
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

                    if (matriculas.size() > 0) {
                        matriculasResponsaveis.add(new ArrayList<String>(matriculas));
                    }
                    matriculas.clear();
                }
            }
        }

        return;
    }
}