package com.yoclabo.database.mysql;

import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLAccessorTest {

    @Test
    public void test01() {
        try {
            MySQLAccessor a = new MySQLAccessor();
            a.setUserId("TEST01");
            a.setPassword("test01");
            a.setServer("192.168.206.129");
            a.setDataSource("TEST01");
            a.open();
            a.close();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void test02() {
        try {
            MySQLAccessor a = initTestEnvironment();
            StringBuilder sb = new StringBuilder();
            sb.append(" \n")
                    .append(" INSERT INTO          \n")
                    .append("     TEST02           \n")
                    .append("     (                \n")
                    .append("     ID               \n")
                    .append("   , NAME             \n")
                    .append("     )                \n")
                    .append(" VALUES               \n")
                    .append("     (                \n")
                    .append("     1                \n")
                    .append("   , 'test02'         \n")
                    .append("     )                \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            a.executeQuery();
            sb = new StringBuilder();
            sb.append(" \n")
                    .append(" SELECT               \n")
                    .append("     COUNT(ID)        \n")
                    .append(" FROM                 \n")
                    .append("     TEST02           \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            Assert.assertEquals(1, (long) a.executeScalar());
            a.close();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void test03() {
        try {
            MySQLAccessor a = initTestEnvironment();
            StringBuilder sb = new StringBuilder();
            sb.append(" \n")
                    .append(" INSERT INTO          \n")
                    .append("     TEST02           \n")
                    .append("     (                \n")
                    .append("     ID               \n")
                    .append("   , NAME             \n")
                    .append("     )                \n")
                    .append(" VALUES               \n")
                    .append("     (                \n")
                    .append("     ?                \n")
                    .append("   , ?                \n")
                    .append("     )                \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            PreparedStatement ps = a.createStatement();
            ps.setInt(1, 2);
            ps.setString(2, "test03-1");
            a.executeQuery(ps);
            ps.clearParameters();
            ps.setInt(1, 3);
            ps.setString(2, "test03-2");
            a.executeQuery(ps);
            sb = new StringBuilder();
            sb.append(" \n")
                    .append(" SELECT               \n")
                    .append("     COUNT(ID)        \n")
                    .append(" FROM                 \n")
                    .append("     TEST02           \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            Assert.assertEquals(2, (long) a.executeScalar());
            sb = new StringBuilder();
            sb.append(" \n")
                    .append(" SELECT               \n")
                    .append("     NAME             \n")
                    .append(" FROM                 \n")
                    .append("     TEST02           \n")
                    .append(" WHERE                \n")
                    .append("     ID = ?           \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            ps = a.createStatement();
            ps.setInt(1, 3);
            Assert.assertEquals("test03-2", a.executeScalar(ps));
            a.close();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void test04() {
        try {
            MySQLAccessor a = initTestEnvironment();
            StringBuilder sb = new StringBuilder();
            sb.append(" \n")
                    .append(" INSERT INTO          \n")
                    .append("     TEST02           \n")
                    .append("     (                \n")
                    .append("     ID               \n")
                    .append("   , NAME             \n")
                    .append("     )                \n")
                    .append(" VALUES               \n")
                    .append("     (                \n")
                    .append("     ?                \n")
                    .append("   , ?                \n")
                    .append("     )                \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            PreparedStatement ps = a.createStatement();
            ps.setInt(1, 4);
            ps.setString(2, "test04-1");
            ps.addBatch();
            ps.setInt(1, 5);
            ps.setString(2, "test04-2");
            ps.addBatch();
            ps.setInt(1, 6);
            ps.setString(2, "test04-3");
            ps.addBatch();
            a.executeBatch(ps);
            sb = new StringBuilder();
            sb.append(" \n")
                    .append(" SELECT               \n")
                    .append("     COUNT(ID)        \n")
                    .append(" FROM                 \n")
                    .append("     TEST02           \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            ResultSet rs = a.executeSelect();
            rs.next();
            Assert.assertEquals(3, (long) rs.getInt(1));
            sb = new StringBuilder();
            sb.append(" \n")
                    .append(" SELECT               \n")
                    .append("     ID               \n")
                    .append("   , NAME             \n")
                    .append(" FROM                 \n")
                    .append("     TEST02           \n")
                    .append(" WHERE                \n")
                    .append("     ID = ?           \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            ps = a.createStatement();
            ps.setInt(1, 4);
            rs = a.executeSelect(ps);
            rs.next();
            Assert.assertEquals("test04-1", rs.getString("NAME"));
            a.close();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void test05() {
        try {
            MySQLAccessor a = initTestEnvironment();
            a.begin();
            StringBuilder sb = new StringBuilder();
            sb.append(" \n")
                    .append(" INSERT INTO          \n")
                    .append("     TEST02           \n")
                    .append("     (                \n")
                    .append("     ID               \n")
                    .append("   , NAME             \n")
                    .append("     )                \n")
                    .append(" VALUES               \n")
                    .append("     (                \n")
                    .append("     ?                \n")
                    .append("   , ?                \n")
                    .append("     )                \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            PreparedStatement ps = a.createStatement();
            ps.setInt(1, 7);
            ps.setString(2, "test05-1");
            ps.addBatch();
            ps.setInt(1, 8);
            ps.setString(2, "test05-2");
            ps.addBatch();
            ps.setInt(1, 9);
            ps.setString(2, "test05-3");
            ps.addBatch();
            a.executeBatch(ps);
            a.commit();
            a.begin();
            ps.clearParameters();
            ps.setInt(1, 10);
            ps.setString(2, "test05-4");
            ps.addBatch();
            ps.setInt(1, 11);
            ps.setString(2, "test05-5");
            ps.addBatch();
            ps.setInt(1, 12);
            ps.setString(2, "test05-6");
            ps.addBatch();
            a.executeBatch(ps);
            a.commit();
            a.begin();
            ps.clearParameters();
            ps.setInt(1, 13);
            ps.setString(2, "test05-7");
            ps.addBatch();
            ps.setInt(1, 14);
            ps.setString(2, "test05-8");
            ps.addBatch();
            ps.setInt(1, 15);
            ps.setString(2, "test05-9");
            ps.addBatch();
            a.executeBatch(ps);
            a.rollback();
            sb = new StringBuilder();
            sb.append(" \n")
                    .append(" SELECT               \n")
                    .append("     COUNT(ID)        \n")
                    .append(" FROM                 \n")
                    .append("     TEST02           \n")
                    .append(" ;                    \n");
            a.setQuery(sb.toString());
            ResultSet rs = a.executeSelect();
            rs.next();
            Assert.assertEquals(6, (long) rs.getInt(1));
            a.close();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    private MySQLAccessor initTestEnvironment() throws ClassNotFoundException, SQLException {
        MySQLAccessor a = new MySQLAccessor();
        a.setUserId("TEST01");
        a.setPassword("test01");
        a.setServer("192.168.206.129");
        a.setDataSource("TEST01");
        a.open();
        StringBuilder sb = new StringBuilder();
        sb.append(" \n")
                .append(" CREATE TABLE         \n")
                .append(" IF NOT EXISTS        \n")
                .append("     TEST02           \n")
                .append("     (                \n")
                .append("     ID int           \n")
                .append("   , NAME VARCHAR(12) \n")
                .append("     )                \n")
                .append(" ;                    \n");
        a.setQuery(sb.toString());
        a.executeQuery();
        sb = new StringBuilder();
        sb.append(" \n")
                .append(" TRUNCATE TABLE       \n")
                .append("     TEST02           \n")
                .append(" ;                    \n");
        a.setQuery(sb.toString());
        a.executeQuery();
        return a;
    }
}
