import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AppWritable3 implements Writable {

    private int n;
    private String commodity;

    public AppWritable3(){

    }

    public AppWritable3(int n, String commodity){
        this.n = n;
        this.commodity = commodity;
    }

    public int getN(){ return this.n;}

    public String getCommodity(){ return this.commodity;}

    @Override
    public void readFields(DataInput in) throws IOException {

        n = Integer.parseInt(in.readUTF());
        commodity = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(commodity));
    }

    @Override
    public String toString() {
        return "AppWritable3{" +
                "n=" + n +
                ", commodity='" + commodity + '\'' +
                '}';
    }
}
