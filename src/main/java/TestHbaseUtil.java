import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

public class TestHbaseUtil implements HBaseUtil{

    private final Connection connection;

    public TestHbaseUtil() throws IOException {
        this("127.0.0.1","2181");
    }

    public TestHbaseUtil(String quorum, String port) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort",port);
        connection = ConnectionFactory.createConnection(conf);
    }


    @Override
    public void createTable(String tableName, String[] fields) throws IOException {
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(tableName);
        if(admin.tableExists(table)){
            admin.disableTable(table);
            admin.deleteTable(table);
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(table);

        List<ColumnFamilyDescriptor> columnFamilies = new ArrayList<>();

        for(String field : fields)
            columnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder(field.getBytes(StandardCharsets.UTF_8)).build());

        builder.setColumnFamilies(columnFamilies);

        admin.createTable(builder.build());

        admin.close();
    }

    @Override
    public void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));


        Put put = new Put(row.getBytes(StandardCharsets.UTF_8));

        int length = Math.min(fields.length, values.length);

        for(int i = 0 ;i < length;i++){
            String[] familyAndQualifier = fields[i].split(":");
            put.addColumn(familyAndQualifier[0].getBytes(),familyAndQualifier[1].getBytes(),values[i].getBytes());
        }

        table.put(put);
        table.close();


    }

    @Override
    public List<String> scanColumn(String tableName, String column) throws IOException {
        Scan scan = new Scan();
        String[] familyAndQualifier = column.split(":");
        if(familyAndQualifier.length != 1 && familyAndQualifier.length != 2)
            throw new IOException("Wrong column formal.");

        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        List<String> resultList = new ArrayList<>();
        if(familyAndQualifier.length == 2){
            for(Result result : scanner){
                Cell cell = result.getColumnLatestCell(familyAndQualifier[0].getBytes(),familyAndQualifier[1].getBytes(StandardCharsets.UTF_8));
                if(cell == null){
                    resultList.add(null);
                    continue;
                }
                resultList.add(new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
            }
        }else {
            for(Result result : scanner){
                NavigableMap<byte[],byte[]> qualifiers = result.getFamilyMap(column.getBytes());
                for(byte[] qualifier : qualifiers.values()){
                    resultList.add(new String(qualifier));
                }
            }
        }
        return resultList.size() == 0 ? null : resultList;
    }

    @Override
    public void modifyData(String tableName, String row, String column, String value) throws IOException {

         addRecord(tableName,row,new String[]{column}, new String[]{value});

    }

    @Override
    public void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(row.getBytes());

        table.delete(delete);

    }

    public static void main(String[] args) throws IOException {

        HBaseUtil hBaseUtil = new TestHbaseUtil();
        final String  tableName = "Student";

        hBaseUtil.createTable(tableName,new String[]{"info","score"});

        hBaseUtil.addRecord(tableName,"Zhangsan",new String[]{"info:id","info:sex","info:age"}, new String[]{"2015001","male","23"});
        hBaseUtil.addRecord(tableName,"Marry",new String[]{"info:id","info:sex","info:age"}, new String[]{"2015002","female","22"});
        hBaseUtil.addRecord(tableName,"Lisi",new String[]{"info:id","info:sex","info:age"}, new String[]{"2015003","male","24"});


        hBaseUtil.addRecord(tableName,"Zhangsan",new String[]{"score:Math","score:English"}, new String[]{"86","69"});
        hBaseUtil.addRecord(tableName,"Marry",new String[]{"score:ComputerScience","score:English"}, new String[]{"77","99"});
        hBaseUtil.addRecord(tableName,"Lisi",new String[]{"score:Math","score:ComputerScience"}, new String[]{"98","95"});


        List<String> r = hBaseUtil.scanColumn(tableName,"score:Math");


        for(String s : r)
            System.out.println(s);

        System.out.println("-------");

        r = hBaseUtil.scanColumn(tableName,"score");

        for(String s : r)
            System.out.println(s);

        hBaseUtil.deleteRow(tableName,"Zhangsan");
        hBaseUtil.modifyData(tableName,"Lisi","score:Math","199999");

        System.out.println("-------");

        r = hBaseUtil.scanColumn(tableName,"score");

        for(String s : r)
            System.out.println(s);
    }

}
