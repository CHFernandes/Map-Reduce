import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;

public class App2 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "TDE-estudante");

        //registra as classes
        j.setJarByClass(App2.class);
        j.setMapperClass(AppMapper2.class);
        j.setReducerClass(AppReducer2.class);

        // definição dos tipos de saída
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(IntWritable.class);

        // passando para o job os arquivos de entrada e saídaa
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

}

