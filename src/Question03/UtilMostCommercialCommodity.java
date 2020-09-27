package Question03;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UtilMostCommercialCommodity implements Writable {

    private String codeCommodity;
    private int cont;

    public UtilMostCommercialCommodity() {
    }

    public UtilMostCommercialCommodity(String codeCommodity, int cont) {
        this.codeCommodity = codeCommodity;
        this.cont = cont;
    }

    public String getCodeCommodity() {
        return codeCommodity;
    }

    public void setCodeCommodity(String codeCommodity) {
        this.codeCommodity = codeCommodity;
    }

    public int getCont() {
        return cont;
    }

    public void setCont(int cont) {
        this.cont = cont;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(codeCommodity);
        out.writeUTF(String.valueOf(cont));

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        codeCommodity = in.readUTF();
        cont = Integer.parseInt(in.readUTF());

    }

    @Override
    public String toString() {
        return "codeCommodity=" + codeCommodity + "  cont=" + cont;
    }
}
