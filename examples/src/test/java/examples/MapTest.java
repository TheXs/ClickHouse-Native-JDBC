package examples;

import com.github.housepower.jdbc.ClickHouseStruct;
import com.github.housepower.jdbc.data.ColumnMap;
import com.github.housepower.jdbc.data.IDataType;
import com.github.housepower.jdbc.data.type.complex.DataTypeString;
import com.github.housepower.jdbc.data.type.complex.DataTypeTuple;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MapTest {

    @Test
    public void testQuery() throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        try (final Connection connection =
                     DriverManager.getConnection("jdbc:clickhouse://192.168.30.12:9000")) {
            final PreparedStatement prepareStatement = connection.prepareStatement("select kv from map");
            final ResultSet resultSet = prepareStatement.executeQuery();
            while (resultSet.next()) {
                Object dataMap = resultSet.getObject(1);
                System.out.println(dataMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsert() throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        final Object[][] data = new Object[1][2];
        data[0] = new Object[] {"name", "test"};
        final ClickHouseStruct struct = new ClickHouseStruct("Map", data);
        try (final Connection connection =
                     DriverManager.getConnection("jdbc:clickhouse://192.168.30.12:9000")) {
            final PreparedStatement prepareStatement =
                    connection.prepareStatement(
                            "insert into map values(?)");
            prepareStatement.setObject(1, struct);
            final int effectCnt = prepareStatement.executeUpdate();
            System.out.println(effectCnt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
