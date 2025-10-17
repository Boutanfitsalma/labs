package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (args.length < 2) {
            System.out.println("Usage: WriteHDFS <hdfs_path> <text_to_write>");
            fs.close();
            return;
        }

        Path nomcomplet = new Path(args[0]);

        if (!fs.exists(nomcomplet)) {
            FSDataOutputStream outStream = fs.create(nomcomplet);
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(args[1]);
            outStream.close();
            System.out.println("File created at: " + args[0]);
        } else {
            System.out.println("File already exists: " + args[0]);
        }

        fs.close();
    }
}

