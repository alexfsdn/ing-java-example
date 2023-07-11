package utils;

import model.Responsavel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import utils.HierarquiaUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class HierarquiaUtilTest {

    private Map<String, List<Responsavel>> mapResponsavel;

    public static final String MATRICULA_T3300045 = "T3300045";
    public static final String MATRICULA_T3300044 = "T3300044";
    public static final String MATRICULA_T3300043 = "T3300043";
    public static final String MATRICULA_T3300042 = "T3300042";
    public static final String MATRICULA_T3300041 = "T3300041";
    public static final String MATRICULA_T3300040 = "T3300040";
    public static final String MATRICULA_T3300039 = "T3300039";
    public static final String MATRICULA_T3300038 = "T3300038";
    public static final String MATRICULA_T3300037 = "T3300037";
    public static final String MATRICULA_T3300036 = "T3300036";


    @Before
    public void setUp() {
        mapResponsavel = mockMapResponsavel();
    }

    @After
    public void cleanUp() {
        mapResponsavel.clear();
    }

    @Test
    public void hierarquiaBuildTest() {
        final int TAMANHO_HIERARQUIA_37 = 2;
        final int TAMANHO_HIERARQUIA_36 = 3;
        final int TAMANHO_HIERARQUIA_44 = 1;
        final int TAMANHO_HIERARQUIA_43 = 2;
        final int TAMANHO_HIERARQUIA_42 = 3;
        final int TAMANHO_HIERARQUIA_41 = 5;
        final int TAMANHO_HIERARQUIA_40 = 7;

        List<String> m37 = HierarquiaUtil.hierarquiaBuild(MATRICULA_T3300037, mapResponsavel);
        List<String> m36 = HierarquiaUtil.hierarquiaBuild(MATRICULA_T3300036, mapResponsavel);
        List<String> m44 = HierarquiaUtil.hierarquiaBuild(MATRICULA_T3300044, mapResponsavel);
        List<String> m43 = HierarquiaUtil.hierarquiaBuild(MATRICULA_T3300043, mapResponsavel);
        List<String> m42 = HierarquiaUtil.hierarquiaBuild(MATRICULA_T3300042, mapResponsavel);
        List<String> m41 = HierarquiaUtil.hierarquiaBuild(MATRICULA_T3300041, mapResponsavel);
        List<String> m40 = HierarquiaUtil.hierarquiaBuild(MATRICULA_T3300040, mapResponsavel);

        Assert.assertEquals(TAMANHO_HIERARQUIA_37, m37.size());
        Assert.assertEquals(TAMANHO_HIERARQUIA_36, m36.size());
        Assert.assertEquals(TAMANHO_HIERARQUIA_44, m44.size());
        Assert.assertEquals(TAMANHO_HIERARQUIA_43, m43.size());
        Assert.assertEquals(TAMANHO_HIERARQUIA_42, m42.size());
        Assert.assertEquals(TAMANHO_HIERARQUIA_41, m41.size());
        Assert.assertEquals(TAMANHO_HIERARQUIA_40, m40.size());


        System.out.println("------------");
        System.out.println("------------");
        System.out.println("------------");
        System.out.println("matricula|matricula_resp");

        for (String r : m37) {
            System.out.println(MATRICULA_T3300037 + "|" + r);
        }
        System.out.println("------------");
        for (String r : m36) {
            System.out.println(MATRICULA_T3300036 + "|" + r);
        }
        System.out.println("------------");
        for (String r : m44) {
            System.out.println(MATRICULA_T3300044 + "|" + r);
        }
        System.out.println("------------");
        for (String r : m43) {
            System.out.println(MATRICULA_T3300043 + "|" + r);
        }
        System.out.println("------------");
        for (String r : m42) {
            System.out.println(MATRICULA_T3300042 + "|" + r);
        }
        System.out.println("------------");
        for (String r : m41) {
            System.out.println(MATRICULA_T3300041 + "|" + r);
        }
        System.out.println("------------");
        for (String r : m40) {
            System.out.println(MATRICULA_T3300040 + "|" + r);
        }


    }

    public Map<String, List<Responsavel>> mockMapResponsavel() {

        Map<String, List<Responsavel>> mapResponsavel = new HashMap<>();

        List<Responsavel> resp37 = new ArrayList<>();
        resp37.add(mockResponsavel2());
        mapResponsavel.put(MATRICULA_T3300037, resp37);

        List<Responsavel> resp36 = new ArrayList<>();
        resp36.add(mockResponsavel3());
        mapResponsavel.put(MATRICULA_T3300036, resp36);

        List<Responsavel> resp38 = new ArrayList<>();
        resp38.add(mockResponsavel1());
        mapResponsavel.put(MATRICULA_T3300038, resp38);

        List<Responsavel> resp40 = new ArrayList<>();
        resp40.add(mockResponsavel8());
        resp40.add(mockResponsavel9());
        mapResponsavel.put(MATRICULA_T3300040, resp40);

        List<Responsavel> resp41 = new ArrayList<>();
        resp41.add(mockResponsavel7());
        resp41.add(mockResponsavel10());
        mapResponsavel.put(MATRICULA_T3300041, resp41);

        List<Responsavel> resp43 = new ArrayList<>();
        resp43.add(mockResponsavel5());
        mapResponsavel.put(MATRICULA_T3300043, resp43);

        List<Responsavel> resp42 = new ArrayList<>();
        resp42.add(mockResponsavel6());
        mapResponsavel.put(MATRICULA_T3300042, resp42);

        List<Responsavel> resp44 = new ArrayList<>();
        resp44.add(mockResponsavel4());
        mapResponsavel.put(MATRICULA_T3300044, resp44);

        return mapResponsavel;
    }

    public Responsavel mockResponsavel1() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300038);
        responsavel.setMatriculaResp(MATRICULA_T3300039);
        return responsavel;
    }

    public Responsavel mockResponsavel2() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300037);
        responsavel.setMatriculaResp(MATRICULA_T3300038);
        return responsavel;
    }

    public Responsavel mockResponsavel3() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300036);
        responsavel.setMatriculaResp(MATRICULA_T3300037);
        return responsavel;
    }

    public Responsavel mockResponsavel4() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300044);
        responsavel.setMatriculaResp(MATRICULA_T3300045);
        return responsavel;
    }


    public Responsavel mockResponsavel5() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300043);
        responsavel.setMatriculaResp(MATRICULA_T3300044);
        return responsavel;
    }

    public Responsavel mockResponsavel6() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300042);
        responsavel.setMatriculaResp(MATRICULA_T3300043);
        return responsavel;
    }

    public Responsavel mockResponsavel7() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300041);
        responsavel.setMatriculaResp(MATRICULA_T3300042);
        return responsavel;
    }

    public Responsavel mockResponsavel8() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300040);
        responsavel.setMatriculaResp(MATRICULA_T3300041);
        return responsavel;
    }

    public Responsavel mockResponsavel9() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300040);
        responsavel.setMatriculaResp(MATRICULA_T3300039);
        return responsavel;
    }

    public Responsavel mockResponsavel10() {
        Responsavel responsavel = new Responsavel();
        responsavel.setMatricula(MATRICULA_T3300041);
        responsavel.setMatriculaResp(MATRICULA_T3300039);
        return responsavel;
    }

}