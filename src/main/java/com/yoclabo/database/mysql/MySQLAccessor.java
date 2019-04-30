/*
 *
 * MySQLAccessor.java
 *
 * Copyright 2019 Yuichi Yoshii
 *     吉井雄一 @ 吉井産業  you.65535.kir@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.yoclabo.database.mysql;

import java.sql.*;

public class MySQLAccessor {

    private String userId;
    private String password;
    private String server;
    private String dataSource;

    private Connection myConnection;
    private boolean transactionBegun;

    private String query;

    public MySQLAccessor() throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
    }

    public void setUserId(String arg) {
        userId = arg;
    }

    public void setPassword(String arg) {
        password = arg;
    }

    public void setServer(String arg) {
        server = arg;
    }

    public void setDataSource(String arg) {
        dataSource = arg;
    }

    private String getConnectionString() {
        return "jdbc:mysql://" + server + "/" + dataSource;
    }

    public boolean isTransactionBegun() { return transactionBegun; }

    public void setQuery(String arg) {
        query = arg;
    }

    public void open() throws SQLException {
        myConnection = DriverManager.getConnection(getConnectionString(), userId, password);
        transactionBegun = false;
    }

    public void begin() throws SQLException {
        myConnection.prepareStatement("BEGIN;").execute();
        transactionBegun = true;
    }

    public PreparedStatement createStatement() throws SQLException {
        return myConnection.prepareStatement(query);
    }

    public ResultSet executeSelect() throws SQLException {
        return myConnection.prepareStatement(query).executeQuery();
    }

    public ResultSet executeSelect(PreparedStatement ps) throws SQLException {
        return ps.executeQuery();
    }

    public boolean executeQuery() throws SQLException {
        return myConnection.prepareStatement(query).execute();
    }

    public boolean executeQuery(PreparedStatement ps) throws SQLException {
        return ps.execute();
    }

    public int[] executeBatch(PreparedStatement ps) throws SQLException {
        return ps.executeBatch();
    }

    public Object executeScalar() throws SQLException {
        ResultSet rs = myConnection.prepareStatement(query).executeQuery();
        rs.next();
        return rs.getObject(1);
    }

    public Object executeScalar(PreparedStatement ps) throws SQLException {
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getObject(1);
    }

    public void commit() throws SQLException {
        myConnection.prepareStatement("COMMIT;").execute();
        transactionBegun = false;
    }

    public void rollback() throws SQLException {
        myConnection.prepareStatement("ROLLBACK;").execute();
        transactionBegun = false;
    }

    public void close() throws SQLException {
        if (null == myConnection) {
            return;
        }
        if (myConnection.isClosed()) {
            return;
        }
        if (transactionBegun) {
            rollback();
        }
        myConnection.close();
    }
}
