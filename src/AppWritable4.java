import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AppWritable4 implements Writable {

    private int n;
    private double peso;

    public AppWritable4(){

    }

    public AppWritable4(int n, double peso){
        this.n = n;
        this.peso = peso;
    }

    public int getN(){ return this.n;}

    public double getPeso(){ return this.peso;}

    @Override
    public void readFields(DataInput in) throws IOException {

        n = Integer.parseInt(in.readUTF());
        peso = Double.parseDouble(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(peso));
    }

    @Override
    public String toString() {
        return "AppWritable4{" +
                "n=" + n +
                ", peso='" + peso + '\'' +
                '}';
    }
}
