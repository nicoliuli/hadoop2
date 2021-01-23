package hbase.day01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HelloWorld {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();


        get(connection,"stu","100001");

        admin.close();
        connection.close();

    }


    public static void createTable(Admin admin, String tableName) throws IOException {
        if (isTableExist(admin, tableName)) {
            return;
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        desc.addFamily(hColumnDescriptor);

        admin.createTable(desc);

    }

    public static boolean isTableExist(Admin admin, String tableName) throws IOException {
        boolean ok = admin.tableExists(TableName.valueOf(tableName));


        System.out.println(ok);
        return ok;

    }

    public static void put(Connection connection, String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        Table stu = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        stu.put(put);
        stu.close();
    }

    public static void get(Connection connection,String tableName,String rowKey) throws IOException {
        Table stu = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = stu.get(get);

        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            // 可以获取列族，列，值等信息，在此处不一一列举
            byte[] bytes = CellUtil.cloneValue(cell);
            System.out.println(Bytes.toString(bytes));
        }
    }
}
