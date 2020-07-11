/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard.db.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A generic database adapter that generates MySQL compatible SQL.
 */
public class PostgreSQLDatabaseAdapter extends GenericDatabaseAdapter {
    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public String getDescription() {
        return "Generates PostgreSQL compatible SQL";
    }

    @Override
    public String unwrapIdentifier(String identifier) {
        // Removes double quotes and back-ticks.
        return identifier == null ? null : identifier.replaceAll("[\"`]", "");
    }

    @Override
    public Statement getStatement(Connection con) throws SQLException {
        return con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.FETCH_FORWARD);
    }

    @Override
    public void setFetchSize(Connection con,final Statement statement,Integer fetchSize) throws SQLException {
        if (fetchSize != null && fetchSize > 0) {
            this.setAutoCommit(con,false);
            statement.setFetchSize(fetchSize);
        }
    }

    @Override
    public void setAutoCommit(Connection con, boolean autoCommit) throws SQLException {
        con.setAutoCommit(autoCommit);
    }
}
