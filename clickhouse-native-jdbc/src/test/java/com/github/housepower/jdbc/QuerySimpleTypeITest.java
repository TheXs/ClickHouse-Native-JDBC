package com.github.housepower.jdbc;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.*;

public class QuerySimpleTypeITest extends AbstractITest {

    @Test
    public void successfullyByName() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT toInt8(" + Byte.MIN_VALUE + ") as a , toUInt8(" + Byte.MAX_VALUE + ") as b");

            assertTrue(rs.next());
            assertEquals(Byte.MIN_VALUE, rs.getByte("a"));
            assertEquals(Byte.MAX_VALUE, rs.getByte("b"));
        });
    }

    @Test
    public void successfullyByteColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement
                    .executeQuery("SELECT toInt8(" + Byte.MIN_VALUE + "), toUInt8(" + Byte.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Byte.MIN_VALUE, rs.getByte(1));
            assertEquals(Byte.MAX_VALUE, rs.getByte(2));
        });
    }

    @Test
    public void successfullyShortColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement
                    .executeQuery("SELECT toInt16(" + Short.MIN_VALUE + "), toUInt16(" + Short.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Short.MIN_VALUE, rs.getShort(1));
            assertEquals(Short.MAX_VALUE, rs.getShort(2));
        });
    }

    @Test
    public void successfullyIntColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement
                    .executeQuery("SELECT toInt32(" + Integer.MIN_VALUE + "), toUInt32(" + Integer.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Integer.MIN_VALUE, rs.getInt(1));
            assertEquals(Integer.MAX_VALUE, rs.getInt(2));
        });
    }

    @Test
    public void successfullyUIntColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement.executeQuery("SELECT toUInt32(" + Integer.MAX_VALUE + " + 128)");

            assertTrue(rs.next());
            assertEquals((long) Integer.MAX_VALUE + 128L, rs.getLong(1));
        });
    }

    @Test
    public void successfullyLongColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement
                    .executeQuery("SELECT toInt64(" + Long.MIN_VALUE + "), toUInt64(" + Long.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Long.MIN_VALUE, rs.getLong(1));
            assertEquals(Long.MAX_VALUE, rs.getLong(2));
        });
    }

    @Test
    public void successfullyFloatColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement
                    .executeQuery("SELECT toFloat32(" + Float.MIN_VALUE + "), toFloat32(" + Float.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Float.MIN_VALUE, rs.getFloat(1), 0.000000000001);
            assertEquals(Float.MAX_VALUE, rs.getFloat(2), 0.000000000001);
        });
    }

    @Test
    public void successfullyDoubleColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement.executeQuery("SELECT toFloat64(4.9E-32), toFloat64(" + Double.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Double.MIN_VALUE, rs.getDouble(1), 0.000000000001);
            assertEquals(Double.MAX_VALUE, rs.getDouble(2), 0.000000000001);
        });
    }

    @Test
    public void successfullyUUIDColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement.executeQuery("SELECT materialize('01234567-89ab-cdef-0123-456789abcdef')");

            assertTrue(rs.next());
            assertEquals("01234567-89ab-cdef-0123-456789abcdef", rs.getString(1));
        });
    }

    @Test
    public void successfullyMetadata() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT number as a1, toString(number) as a2, now() as a3, today() as a4 from numbers(1)");

            assertTrue(rs.next());
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals("a1", metaData.getColumnName(1));
            assertEquals("UInt64", metaData.getColumnTypeName(1));
            assertEquals("java.lang.Long", metaData.getColumnClassName(1));

            assertEquals("a2", metaData.getColumnName(2));
            assertEquals("String", metaData.getColumnTypeName(2));
            assertEquals("java.lang.String", metaData.getColumnClassName(2));

            assertEquals("a3", metaData.getColumnName(3));
            assertEquals("DateTime", metaData.getColumnTypeName(3));
            assertEquals("java.sql.Timestamp", metaData.getColumnClassName(3));

            assertEquals("a4", metaData.getColumnName(4));
            assertEquals("Date", metaData.getColumnTypeName(4));
            assertEquals("java.sql.Date", metaData.getColumnClassName(4));
        });
    }

    @Test
    public void successfullyStringDataTypeWithSingleQuote() throws Exception {
        withNewConnection(connection -> {
            try (Statement statement = connection.createStatement()) {

                statement.executeQuery("DROP TABLE IF EXISTS test");
                statement.executeQuery("CREATE TABLE test(test String)ENGINE=Log");

                try (PreparedStatement ps = connection.prepareStatement("INSERT INTO test VALUES(?)")) {
                    ps.setString(1, "test_string with ' character");
                    assertEquals(1, ps.executeUpdate());
                }

                try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM test WHERE test=?")) {
                    ps.setString(1, "test_string with ' character");
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals("test_string with ' character", rs.getString(1));
                        assertFalse(rs.next());
                    }
                }
                statement.executeQuery("DROP TABLE IF EXISTS test");
            }
        });
    }

}
