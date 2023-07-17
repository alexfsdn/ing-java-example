package dtos;

import lombok.Data;

@Data
public class LivroDto {

    private Long id;
    private String nome;
    private boolean capaDura;
    private int numeroPaginas;
    private String nomeDaEditora;

    public LivroDto() {

    }

   /* public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public boolean isCapaDura() {
        return capaDura;
    }

    public void setCapaDura(boolean capaDura) {
        this.capaDura = capaDura;
    }

    public int getNumeroPaginas() {
        return numeroPaginas;
    }

    public void setNumeroPaginas(int numeroPaginas) {
        this.numeroPaginas = numeroPaginas;
    }

    public String getNomeDaEditora() {
        return nomeDaEditora;
    }

    public void setNomeDaEditora(String nomeDaEditora) {
        this.nomeDaEditora = nomeDaEditora;
    }

    */
}