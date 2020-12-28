import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;


class FR_table {
    public static void main(String[] args) {
        if ( args.length < 2 ){
            System.out.println("Please input <Table_name> <zk_IP>:<zk_Port>!");
            System.out.println("example:java FR_table.jar Table_name zk_IP:zk_Port");
            System.exit(0);
        }

        String tableName = args[0];
        String zkInfo = args[1];
        String [] zkInfo_arr=zkInfo.split(":");

        String zkIp=zkInfo_arr[0];
        String zkPort=zkInfo_arr[1];

        System.out.println(tableName);

        //SONAR_FIELD_SOURCE:ZIP

        Connection conn = null;
        Table table = null;
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkIp);
        config.set("hbase.zookeeper.property.clientPort", zkPort);
        try {
            conn = ConnectionFactory.createConnection(config);
            Admin admin = conn.getAdmin();

            TableName tableName1 = TableName.valueOf(tableName);
            FR_table fr = new FR_table();
            List l1 = fr.getNormalRegion(tableName1,admin);
            List l2 = fr.getAllRegion(tableName1,admin);
            List repiar_region_list = fr.getRepairRegion(l1,l2);
            fr.assignRegion(repiar_region_list,admin);

            //System.out.println(repiar_region_list);

        } catch (IOException ioe) {
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }
        //System.out.println("l1"+l1);
        //System.out.println("l2"+l2);
    }


    public List getNormalRegion(TableName tableName1, Admin admin) throws IOException {
        ArrayList l1 = new ArrayList();
        Collection<ServerName> regionServers = admin.getRegionServers();
        Iterator<ServerName> its = regionServers.iterator();

        while (its.hasNext()) {
            ServerName serverName = its.next();
            List<RegionMetrics> regionMetrics = admin.getRegionMetrics(serverName, tableName1);
            for (RegionMetrics region : regionMetrics) {
                String size = region.getStoreFileSize().toString();
                String regionName = new String(region.getRegionName());
                l1.add(regionName);
            }
        }
        return l1;
    }

    public List getAllRegion(TableName tableName1,Admin admin) throws IOException {
        ArrayList l2 = new ArrayList();
        List<RegionInfo> regionInfoList = admin.getRegions(tableName1);
        for (RegionInfo regionInfo : regionInfoList) {
            l2.add(regionInfo.getRegionNameAsString());
        }
        return l2;
    }


    public List getRepairRegion(List l1, List l2) {
        Iterator<String> it = l2.iterator();
        while (it.hasNext()) {
            String str = it.next();
            for (int i =0 ; i<l1.size() ; i++) {
                    if (str.equals(l1.get(i))) {
                        it.remove();
                    }
            }
        }
        return l2;
    }

    public void assignRegion(List<String> repiar_region_list,Admin admin) throws IOException {

        for (int i=0;i<repiar_region_list.size();i++) {
            String repair_region = repiar_region_list.get(i);
             admin.assign(repair_region.getBytes());
             System.out.println(repair_region+"assign success!");
        }
    }
}