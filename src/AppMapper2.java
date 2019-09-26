import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AppMapper2 extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context con) throws IOException,
            InterruptedException {
        // conversão do conteudo em string
        String conteudo = value.toString();

        //split por virgula
        String[] palavras = conteudo.split(";");

        if (!palavras[1].equals("year")) {
            int ano = Integer.parseInt(palavras[1]);
            con.write(new IntWritable(ano), new IntWritable(1));
        }
    }
}
