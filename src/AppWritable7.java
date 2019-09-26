import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AppWritable7 implements Writable{

    private String commodity;
    private double media;

    public AppWritable7(){

    }

    public AppWritable7(String commodity, double media){
        this.commodity = commodity;
        this.media = media;
    }

    public String getCommodity() {
        return this.commodity;
    }

    public double getMedia(){
        return this.media;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        commodity = in.readUTF();
        media = Double.valueOf(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(commodity));
        out.writeUTF(String.valueOf(media));
    }

    @Override
    public String toString() {
        return
                "commodity='" + commodity + '\'' +
                ", media=" + media;
    }
}
