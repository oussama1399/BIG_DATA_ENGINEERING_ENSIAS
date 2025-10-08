package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            Path filepath = new Path("/user/root/input", "purchases.txt");
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist");
                System.exit(1);
            }
            FileStatus infos = fs.getFileStatus(filepath);
            System.out.println(Long.toString(infos.getLen()) + " bytes");
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + infos.getLen());
            System.out.println("File owner: " + infos.getOwner());
            System.out.println("File permission: " + infos.getPermission());
            System.out.println("File Replication: " + infos.getReplication());
            System.out.println("File Block Size: " + infos.getBlockSize());
            BlockLocation[] blockLocations = fs.getFileBlockLocations(infos, 0, infos.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            fs.rename(filepath, new Path("/user/root/input", "achats.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}