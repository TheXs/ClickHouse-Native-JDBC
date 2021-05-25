package examples;

import com.github.housepower.jdbc.ClickHouseStruct;
import com.github.housepower.jdbc.data.ColumnMap;
import com.github.housepower.jdbc.data.IDataType;
import com.github.housepower.jdbc.data.type.complex.DataTypeString;
import com.github.housepower.jdbc.data.type.complex.DataTypeTuple;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

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
        final Object[][] data = new Object[2][2];
        data[0] = new Object[] {"name", "test"};
        data[1] = new Object[] {"cop", "24"};
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

    @Test
    public void testInsertBatch() throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        final Random random = new Random();

        final List<Object[][]> dataList = new ArrayList<>();
        dataList.add(new Object[][] {new Object[]{"name", "test"},
                new Object[]{"id", String.valueOf(random.nextInt(100))}});
        dataList.add(new Object[][] {new Object[]{"name", "test"},
                new Object[]{"id", String.valueOf(random.nextInt(100))}});
        dataList.add(new Object[][] {new Object[]{"name", "test"},
                new Object[]{"id", String.valueOf(random.nextInt(100))}});
        dataList.add(new Object[][] {new Object[]{"name", "test"},
                new Object[]{"id", String.valueOf(random.nextInt(100))}});
        dataList.add(new Object[][] {new Object[]{"name", "test"},
                new Object[]{"id", String.valueOf(random.nextInt(100))}});
        try (final Connection connection =
                     DriverManager.getConnection("jdbc:clickhouse://192.168.30.12:9000")) {
            final PreparedStatement prepareStatement =
                    connection.prepareStatement(
                            "insert into map values(?)");
            for (Object[][] data : dataList) {
                prepareStatement.setObject(1, new ClickHouseStruct("Map", data));
                prepareStatement.addBatch();
            }
            final int[] effectCntArr = prepareStatement.executeBatch();
            Arrays.stream(effectCntArr).forEach(System.out::print);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
