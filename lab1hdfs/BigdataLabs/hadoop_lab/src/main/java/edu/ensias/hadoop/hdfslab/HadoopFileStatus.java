
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: HadoopFileStatus <inputPath> <outputPath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Get file status
        Path path = new Path(inputPath);
        FileStatus fileStatus = fs.getFileStatus(path);

        // Check if it's a file
        if (!fileStatus.isFile()) {
            System.err.println("Error: " + inputPath + " is not a file.");
            fs.close();
            System.exit(1);
        }

        // Print file info
        System.out.println("File: " + fileStatus.getPath());
        System.out.println("Size: " + fileStatus.getLen());
        System.out.println("Block size: " + fileStatus.getBlockSize());
        System.out.println("Replication: " + fileStatus.getReplication());
        System.out.println("Modification time: " + fileStatus.getModificationTime());

        // Rename file
        Path newPath = new Path(outputPath);
        boolean renamed = fs.rename(path, newPath);
        if (renamed) {
            System.out.println("File renamed successfully to " + outputPath);
        } else {
            System.out.println("Failed to rename file");
        }

        fs.close();
    }
}
