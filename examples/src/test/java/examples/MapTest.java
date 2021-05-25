package examples;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MapTest {

    @Test
    public void testBase() throws Exception {
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
}
