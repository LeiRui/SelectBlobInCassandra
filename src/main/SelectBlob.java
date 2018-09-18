package main;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;

public class SelectBlob {
    private static String nodes = "127.0.0.1";
    public static void main(String[] args) {
//        int limit = Integer.parseInt(args[0]);
        int limit = 1000;
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream("selectBlob2.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Cluster cluster = Cluster.builder().addContactPoint(nodes).build();
        Session session = cluster.connect();
        String q_format = "select value from micapsdataserver.\"ECMWF_HR\" where \"dataPath\"" +
                "='FRACTION_OF_CLOUD_COVER/%d'" +
                " limit "+limit+";";

        for(int j=1;j<=137;j++) {
            String q = String.format(q_format,j);
            ResultSet rs = session.execute(q);
            Iterator<Row> iter = rs.iterator();
            while (iter.hasNext()) {
                Row row = iter.next();
                byte[] value = Bytes.getArray(row.getBytes("value"));
                int len = value.length;
                pw.println(len);
            }
        }

        pw.close();
        session.close();
        cluster.close();
    }
}
