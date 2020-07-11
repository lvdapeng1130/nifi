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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.processors.standard.db.impl.OracleDatabaseAdapter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the QueryDatabaseTable processor
 */
public class TestQueryDatabaseTable {

    MockQueryDatabaseTable processor;
    private TestRunner runner;
    private final static String DB_LOCATION = "target/db_qdt";
    private DatabaseAdapter dbAdapter;
    private HashMap<String, DatabaseAdapter> origDbAdapters;
    private final static String TABLE_NAME_KEY = "tableName";
    private final static String MAX_ROWS_KEY = "maxRows";


    @Before
    public void setup() throws InitializationException, IOException {
        final DBCPService oracle = new OracleServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();
        origDbAdapters = new HashMap<>(QueryDatabaseTable.dbAdapters);
        dbAdapter = new OracleDatabaseAdapter();
        QueryDatabaseTable.dbAdapters.put(dbAdapter.getName(), dbAdapter);
        processor = new MockQueryDatabaseTable();
        runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        runner.addControllerService("oracle", oracle, dbcpProperties);
        runner.enableControllerService(oracle);
        runner.setProperty(QueryDatabaseTable.DBCP_SERVICE, "oracle");
        runner.setProperty(QueryDatabaseTable.DB_TYPE, dbAdapter.getName());
        runner.getStateManager().clear(Scope.CLUSTER);
    }

    @After
    public void teardown() throws IOException {
        runner.getStateManager().clear(Scope.CLUSTER);
        runner = null;
        QueryDatabaseTable.dbAdapters.clear();
        QueryDatabaseTable.dbAdapters.putAll(origDbAdapters);
    }

    @Test
    public void testAddedRowsCustomWhereClause() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        runner.setProperty(QueryDatabaseTable.TABLE_NAME, "V_DOC");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE,"50");
        runner.setProperty(QueryDatabaseTable.FETCH_SIZE,"200");
        runner.setProperty(QueryDatabaseTable.OUTPUT_BATCH_SIZE,"300");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTable.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTable.REL_SUCCESS).get(0);
        runner.clearTransferState();
    }

    /**
     * Simple implementation only for QueryDatabaseTable processor testing.
     */
    private class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    private class OracleServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "oracle";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                return DriverManager.getConnection("jdbc:oracle:thin:@192.168.5.152:1521:orcl","ky_data","kydata_152");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    private class MySqlServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "MySQL";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                return DriverManager.getConnection("jdbc:mysql://192.168.1.40:3306/wikidata?profileSQL=true&useInformationSchema=true","wikidata","wikidata");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    private class PostgreSqlServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "PostgreSQL";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.postgresql.Driver");
                return DriverManager.getConnection("jdbc:postgresql://192.168.1.62:5432/data_store_api","postgres","postgres");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    @Stateful(scopes = Scope.CLUSTER, description = "Mock for QueryDatabaseTable processor")
    private static class MockQueryDatabaseTable extends QueryDatabaseTable {
        void putColumnType(String colName, Integer colType) {
            columnTypeMap.put(colName, colType);
        }
    }
}