import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class RIT_repair {
    public static void main(String args[]){
        Connection conn = null;
        Configuration config = HBaseConfiguration.create();
        if( args.length<1 ){
            System.out.println("please zk info:");
            System.out.println("example: java -cp xx.jar RIT_repair xx.xx.xx.xx:2181");
            System.exit(0);
        }
        String zkInfo = args[0];
        String zkIp=zkInfo.split(":")[0];
        String zkPort=zkInfo.split(":")[1];
        //System.out.println(zkIp+":"+zkPort);
        config.set("hbase.zookeeper.quorum", zkIp);
        config.set("hbase.zookeeper.property.clientPort", zkPort);
        RIT_repair rit = new RIT_repair();
        try {
            conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();

            List rit_list = rit.get_rit_region(admin);
            rit.assign_rit_region(rit_list,admin);

        } catch (IOException ioe) {
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }
    }

    public List get_rit_region(Admin admin) {
        try {
            ClusterStatus status = admin.getClusterStatus();
            return status.getRegionStatesInTransition();
        }catch(IOException ioe){
            throw new RuntimeException();
        }
    }

    public void assign_rit_region(List list,Admin admin)  {
        Iterator it = list.iterator();
        while (it.hasNext()) {
            String region = it.next().toString();
            region = region.split(" ")[0].replace("{", "");
            try {
                admin.assign(region.getBytes());
                System.out.println("assign Success " + region);
            } catch (Exception ioe){
                continue;
            }
        }
    }
}