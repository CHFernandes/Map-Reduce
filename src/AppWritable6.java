import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AppWritable6 implements Writable{

    private int n;
    private double value;

    public AppWritable6(){

    }

    public AppWritable6(int n, double value){
        this.n = n;
        this.value = value;
    }

    public int getN() {
        return this.n;
    }

    public double getValue(){
        return  this.value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = Integer.valueOf(in.readUTF());
        value = Double.valueOf(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(value));
    }

}
