import java.io.IOException;
import java.util.List;

public interface HBaseUtil {
    void createTable(String tableName, String[] fields) throws IOException;
    void addRecord(String tableName, String row,String[] fields, String values[]) throws IOException;
    List<String> scanColumn(String tableName, String column) throws IOException;
    void modifyData(String tableName, String row, String column, String data) throws IOException;
    void deleteRow(String tableName, String row) throws IOException;
}
