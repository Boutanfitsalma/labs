package edu.ensias.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path nomcomplet = new Path(args.length > 0 ? args[0] : "/user/root/input/achats.txt");

        if (!fs.exists(nomcomplet)) {
            System.out.println("File not found: " + nomcomplet);
            fs.close();
            return;
        }

        FSDataInputStream inStream = fs.open(nomcomplet);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        br.close();
        fs.close();
    }
}
