package com.github.housepower.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.Assert.*;

public class QueryRandomITest extends AbstractITest {

    @Test
    public void successfullyDateTime64DataType() throws Exception {
        withNewConnection(connection -> {
            Statement statement = connection.createStatement();

            statement.executeQuery("DROP TABLE IF EXISTS test_random");
            statement.executeQuery("CREATE TABLE test_random "
                                   + "(name String, value UInt32, arr Array(Float64), day Date, time DateTime, dc Decimal(7,2))"
                                   + "ENGINE = GenerateRandom(1, 8, 8)");

            ResultSet rs = statement.executeQuery("SELECT * FROM test_random limit 10000");

            int i = 0;
            while (rs.next()) {
                Object name = rs.getObject(1);
                Object value = rs.getObject(2);
                Object arr = rs.getObject(3);
                Object day = rs.getObject(4);
                Object time = rs.getObject(5);
                Object dc = rs.getObject(6);

                assertEquals(String.class, name.getClass());
                assertEquals(Long.class, value.getClass());
                assertEquals(ClickHouseArray.class, arr.getClass());
                assertEquals(Date.class, day.getClass());
                assertEquals(Timestamp.class, time.getClass());
                assertEquals(BigDecimal.class, dc.getClass());

                i ++;
            }
            assertEquals(i , 10000);
            statement.executeQuery("DROP TABLE IF EXISTS test_random");
        }, true);
    }
}
