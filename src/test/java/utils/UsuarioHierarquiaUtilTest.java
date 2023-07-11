package utils;

import model.Login;
import model.Responsavel;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class UsuarioHierarquiaUtilTest {

    private List<List<String>> listaResponsaveis;
    private Map<String, List<Responsavel>> mapResponsavel;
    private Map<String, List<Login>> mapLogin;

    private static final String EXECUTIVO = "Executivo";
    private static final String GERENTE_REGIONAL = "Gerente Regional";
    private static final String HEAD_NACIONAL = "Head Nacional";
    private static final String DIRETOR_NACIONAL = "Diretor Nacional";
    private static final String EMPRESARIO = "Empresário";
    private static final String DIRETOR_DE_VENDAS = "Diretor de Vendas";

    public static final String MATRICULA_A = "A";
    public static final String MATRICULA_B = "B";
    public static final String MATRICULA_C = "C";
    public static final String MATRICULA_D = "D";
    public static final String MATRICULA_E = "E";
    public static final String MATRICULA_F = "F";
    public static final String MATRICULA_G = "G";
    public static final String MATRICULA_H = "H";
    public static final String MATRICULA_I = "I";
    public static final String MATRICULA_J = "J";

    public static final String CARGO_A = EXECUTIVO;
    public static final String CARGO_B = GERENTE_REGIONAL;
    public static final String CARGO_C = HEAD_NACIONAL;
    public static final String CARGO_D = DIRETOR_NACIONAL;
    public static final String CARGO_E = EMPRESARIO;
    public static final String CARGO_F = DIRETOR_DE_VENDAS;
    public static final String CARGO_G = EMPRESARIO;
    public static final String CARGO_H = EXECUTIVO;
    public static final String CARGO_I = EXECUTIVO;
    public static final String CARGO_J = GERENTE_REGIONAL;


    @Before
    public void setUp() {
        mockMapResponsavelECargo();
    }

    @Test
    public void hierarquiaBuildTest() {

        final String RESULT_ESPERADO_A = "A||B|C|D";
        final String RESULT_ESPERADO_B = "B|||C|D";
        final String RESULT_ESPERADO_C = "C||||D";
        final String RESULT_ESPERADO_D = "D||||";
        final String RESULT_ESPERADO_E = "E|A; H; I|B|C|D";
        final String RESULT_ESPERADO_F = "F|||C|D";
        final String RESULT_ESPERADO_G = "G|H; I; A|B|C|D";
        final String RESULT_ESPERADO_H = "H|I; A|B|C|D";
        final String RESULT_ESPERADO_I = "I|A|B|C|D";
        final String RESULT_ESPERADO_J = "J||||";

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_A, mapResponsavel);
        String resultA = hierarquiaOut(MATRICULA_A);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_B, mapResponsavel);
        String resultB = hierarquiaOut(MATRICULA_B);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_C, mapResponsavel);
        String resultC = hierarquiaOut(MATRICULA_C);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_D, mapResponsavel);
        String resultD = hierarquiaOut(MATRICULA_D);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_E, mapResponsavel);
        String resultE = hierarquiaOut(MATRICULA_E);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_F, mapResponsavel);
        String resultF = hierarquiaOut(MATRICULA_F);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_G, mapResponsavel);
        String resultG = hierarquiaOut(MATRICULA_G);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_H, mapResponsavel);
        String resultH = hierarquiaOut(MATRICULA_H);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_I, mapResponsavel);
        String resultI = hierarquiaOut(MATRICULA_I);

        listaResponsaveis = UsuarioHierarquiaUtil.hierarquiaBuild(MATRICULA_J, mapResponsavel);
        String resultJ = hierarquiaOut(MATRICULA_J);

        Assert.assertEquals(RESULT_ESPERADO_A, resultA);
        Assert.assertEquals(RESULT_ESPERADO_B, resultB);
        Assert.assertEquals(RESULT_ESPERADO_C, resultC);
        Assert.assertEquals(RESULT_ESPERADO_D, resultD);
        Assert.assertEquals(RESULT_ESPERADO_E, resultE);
        Assert.assertEquals(RESULT_ESPERADO_F, resultF);
        Assert.assertEquals(RESULT_ESPERADO_G, resultG);
        Assert.assertEquals(RESULT_ESPERADO_H, resultH);
        Assert.assertEquals(RESULT_ESPERADO_I, resultI);
        Assert.assertEquals(RESULT_ESPERADO_J, resultJ);
    }

    private String hierarquiaOut(String matricula) {
        String executivo = StringUtils.EMPTY;
        String gerenteRegional = StringUtils.EMPTY;
        String headNacional = StringUtils.EMPTY;
        String diretorNacional = StringUtils.EMPTY;

        for (int i = 0; i < listaResponsaveis.size(); i++) {
            for (int j = 0; j < listaResponsaveis.get(i).size(); j++) {
                String responsavel = listaResponsaveis.get(i).get(j);
                String valor = Optional.ofNullable(this.mapLogin.get(responsavel)).map(list -> list.get(0)).map(item -> item.getCargo()).orElse(StringUtils.EMPTY);
                if (valor.equals(EXECUTIVO)) {
                    if (executivo.length() > 0) {
                        if (!verificaDuplicidade(executivo, responsavel)) {
                            executivo = executivo + "; " + responsavel;
                        }
                    } else {
                        executivo = responsavel;
                    }
                } else if (valor.equals(GERENTE_REGIONAL)) {
                    if (gerenteRegional.length() > 0) {
                        if (!verificaDuplicidade(gerenteRegional, responsavel)) {
                            gerenteRegional = gerenteRegional + "; " + responsavel;
                        }
                    } else {
                        gerenteRegional = responsavel;
                    }
                } else if (valor.equals(HEAD_NACIONAL)) {
                    if (headNacional.length() > 0) {
                        if (!verificaDuplicidade(headNacional, responsavel)) {
                            headNacional = headNacional + "; " + responsavel;
                        }
                    } else {
                        headNacional = responsavel;
                    }
                } else if (valor.equals(DIRETOR_NACIONAL)) {
                    if (diretorNacional.length() > 0) {
                        if (!verificaDuplicidade(diretorNacional, responsavel)) {
                            diretorNacional = diretorNacional + "; " + responsavel;
                        }
                    } else {
                        diretorNacional = responsavel;
                    }
                }
            }
        }

        return matricula + "|" + executivo + "|" + gerenteRegional + "|" + headNacional + "|" + diretorNacional;


    }

    private boolean verificaDuplicidade(String matricula, String responsavel) {

        String[] matriculas = matricula.toString().split(";", -1);

        if (matriculas.length > 0) {
            for (String m :
                    matriculas) {
                if (m.equals(responsavel)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void mockMapResponsavelECargo() {
        mapResponsavel = new HashMap<>();
        mapLogin = new HashMap<>();

        List<Responsavel> respA = new ArrayList<>();
        respA.add(mockResponsavelA());
        mapResponsavel.put(MATRICULA_A, respA);

        List<Login> loginA = new ArrayList<>();
        loginA.add(mockaLoginA());
        mapLogin.put(MATRICULA_A, loginA);

        List<Responsavel> respB = new ArrayList<>();
        respB.add(mockResponsavelB());
        mapResponsavel.put(MATRICULA_B, respB);

        List<Login> loginB = new ArrayList<>();
        loginB.add(mockaLoginB());
        mapLogin.put(MATRICULA_B, loginB);

        List<Responsavel> respC = new ArrayList<>();
        respC.add(mockResponsavelC());
        mapResponsavel.put(MATRICULA_C, respC);

        List<Login> loginC = new ArrayList<>();
        loginC.add(mockaLoginC());
        mapLogin.put(MATRICULA_C, loginC);

        List<Login> loginD = new ArrayList<>();
        loginD.add(mockaLoginD());
        mapLogin.put(MATRICULA_D, loginD);

        List<Responsavel> respE = new ArrayList<>();
        respE.add(mockResponsavelE());
        mapResponsavel.put(MATRICULA_E, respE);

        List<Login> loginE = new ArrayList<>();
        loginE.add(mockaLoginE());
        mapLogin.put(MATRICULA_E, loginE);

        List<Responsavel> respF = new ArrayList<>();
        respF.add(mockResponsavelF());
        mapResponsavel.put(MATRICULA_F, respF);

        List<Login> loginF = new ArrayList<>();
        loginF.add(mockaLoginF());
        mapLogin.put(MATRICULA_F, loginF);

        List<Responsavel> respG = new ArrayList<>();
        respG.add(mockResponsavelG());
        mapResponsavel.put(MATRICULA_G, respG);

        List<Login> loginG = new ArrayList<>();
        loginG.add(mockaLoginG());
        mapLogin.put(MATRICULA_G, loginG);

        List<Responsavel> respH = new ArrayList<>();
        respH.add(mockResponsavelH());
        mapResponsavel.put(MATRICULA_H, respH);

        List<Login> loginH = new ArrayList<>();
        loginH.add(mockaLoginH());
        mapLogin.put(MATRICULA_H, loginH);

        List<Responsavel> respI = new ArrayList<>();
        respI.add(mockResponsavelI());
        mapResponsavel.put(MATRICULA_I, respI);

        List<Login> loginI = new ArrayList<>();
        loginI.add(mockaLoginI());
        mapLogin.put(MATRICULA_I, loginI);

        List<Login> loginJ = new ArrayList<>();
        loginJ.add(mockaLoginJ());
        mapLogin.put(MATRICULA_J, loginJ);

        //plus
        respE.add(mockResponsavelEComOutroResponsavel());
        mapResponsavel.put(MATRICULA_E, respE);

    }

    private Login mockaLoginA() {
        return new Login(MATRICULA_A, CARGO_A);
    }

    private Login mockaLoginB() {
        return new Login(MATRICULA_B, CARGO_B);
    }

    private Login mockaLoginC() {
        return new Login(MATRICULA_C, CARGO_C);
    }


    private Login mockaLoginD() {
        return new Login(MATRICULA_D, CARGO_D);
    }


    private Login mockaLoginE() {
        return new Login(MATRICULA_E, CARGO_E);
    }

    private Login mockaLoginF() {
        return new Login(MATRICULA_F, CARGO_F);
    }

    private Login mockaLoginG() {
        return new Login(MATRICULA_G, CARGO_G);
    }

    private Login mockaLoginH() {
        return new Login(MATRICULA_H, CARGO_H);
    }

    private Login mockaLoginI() {
        return new Login(MATRICULA_I, CARGO_I);
    }

    private Login mockaLoginJ() {
        return new Login(MATRICULA_J, CARGO_J);
    }

    public Responsavel mockResponsavelA() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_A);
        responsavel.setMatriculaResp(MATRICULA_B);
        return responsavel;
    }

    public Responsavel mockResponsavelB() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_B);
        responsavel.setMatriculaResp(MATRICULA_C);
        return responsavel;
    }

    public Responsavel mockResponsavelC() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_C);
        responsavel.setMatriculaResp(MATRICULA_D);
        return responsavel;
    }

    public Responsavel mockResponsavelE() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_E);
        responsavel.setMatriculaResp(MATRICULA_A);
        return responsavel;
    }

    public Responsavel mockResponsavelEComOutroResponsavel() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_E);
        responsavel.setMatriculaResp(MATRICULA_H);
        return responsavel;
    }

    public Responsavel mockResponsavelF() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_F);
        responsavel.setMatriculaResp(MATRICULA_C);
        return responsavel;
    }

    public Responsavel mockResponsavelG() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_G);
        responsavel.setMatriculaResp(MATRICULA_H);
        return responsavel;
    }

    public Responsavel mockResponsavelH() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_H);
        responsavel.setMatriculaResp(MATRICULA_I);
        return responsavel;
    }

    public Responsavel mockResponsavelI() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_I);
        responsavel.setMatriculaResp(MATRICULA_A);
        return responsavel;
    }

}