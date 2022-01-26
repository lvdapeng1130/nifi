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
package org.apache.nifi.processors.hive;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.hive.Hive3DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.PartialFunctions;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.hive.CsvOutputOptions;
import org.apache.nifi.util.hive.HiveJdbcCommon;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.sql.Types.*;
import static org.apache.nifi.util.hive.HiveJdbcCommon.*;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"hive", "sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("Execute provided HiveQL SELECT query against a Hive database connection. Query result will be converted to Avro or CSV format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'selecthiveql.row.count' indicates how many rows were selected.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the MIME type for the outgoing flowfile to application/avro-binary for Avro or text/csv for CSV."),
        @WritesAttribute(attribute = "filename", description = "Adds .avro or .csv to the filename attribute depending on which output format is selected."),
        @WritesAttribute(attribute = "selecthiveql.row.count", description = "Indicates how many rows were selected/returned by the query."),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "query.input.tables", description = "Contains input table names in comma delimited 'databaseName.tableName' format.")
})
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
public class SelectHive3QL extends AbstractHive3QLProcessor {

    static final String RESULT_ROW_COUNT = "selecthiveql.row.count";

    // Relationships
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from HiveQL query result set.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("HiveQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship.")
            .build();


    public static final PropertyDescriptor HIVEQL_PRE_QUERY = new PropertyDescriptor.Builder()
            .name("hive-pre-query")
            .displayName("HiveQL Pre-Query")
            .description("HiveQL pre-query to execute. Semicolon-delimited list of queries. "
                    + "Example: 'set tez.queue.name=queue1; set hive.exec.orc.split.strategy=ETL; set hive.exec.reducers.bytes.per.reducer=1073741824'. "
                    + "Note, the results/outputs of these queries will be suppressed if successfully executed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor HIVEQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("hive-query")
            .displayName("HiveQL Select Query")
            .description("HiveQL SELECT query to execute. If this is not set, the query is assumed to be in the content of an incoming FlowFile.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor HIVEQL_POST_QUERY = new PropertyDescriptor.Builder()
            .name("hive-post-query")
            .displayName("HiveQL Post-Query")
            .description("HiveQL post-query to execute. Semicolon-delimited list of queries. "
                    + "Note, the results/outputs of these queries will be suppressed if successfully executed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("hive-fetch-size")
            .displayName("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("hive-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. " +
                    "This will allow you to break up very large result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor MAX_FRAGMENTS = new PropertyDescriptor.Builder()
            .name("hive-max-frags")
            .displayName("Maximum Number of Fragments")
            .description("The maximum number of fragments. If the value specified is zero, then all fragments are returned. " +
                    "This prevents OutOfMemoryError when this processor ingests huge table.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor HIVEQL_CSV_HEADER = new PropertyDescriptor.Builder()
            .name("csv-header")
            .displayName("CSV Header")
            .description("Include Header in Output")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor HIVEQL_CSV_ALT_HEADER = new PropertyDescriptor.Builder()
            .name("csv-alt-header")
            .displayName("Alternate CSV Header")
            .description("Comma separated list of header fields")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor HIVEQL_CSV_DELIMITER = new PropertyDescriptor.Builder()
            .name("csv-delimiter")
            .displayName("CSV Delimiter")
            .description("CSV Delimiter used to separate fields")
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor HIVEQL_CSV_QUOTE = new PropertyDescriptor.Builder()
            .name("csv-quote")
            .displayName("CSV Quote")
            .description("Whether to force quoting of CSV fields. Note that this might conflict with the setting for CSV Escape.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVEQL_CSV_ESCAPE = new PropertyDescriptor.Builder()
            .name("csv-escape")
            .displayName("CSV Escape")
            .description("Whether to escape CSV strings in output. Note that this might conflict with the setting for CSV Quote.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor HIVEQL_OUTPUT_FORMAT = new PropertyDescriptor.Builder()
            .name("hive-output-format")
            .displayName("Output Format")
            .description("How to represent the records coming from Hive (Avro, CSV, e.g.)")
            .required(true)
            .allowableValues(AVRO, CSV)
            .defaultValue(AVRO)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor USE_AVRO_LOGICAL_TYPES = new PropertyDescriptor.Builder()
            .name("use-logical-types")
            .displayName("Use Avro Logical Types")
            .description("Whether to use Avro Logical Types for DECIMAL, DATE and TIMESTAMP columns. "
                    + "If disabled, written as string. "
                    + "If enabled, Logical types are used and written as its underlying type, specifically, "
                    + "DECIMAL as logical 'decimal': written as bytes with additional precision and scale meta data, "
                    + "DATE as logical 'date': written as int denoting days since Unix epoch (1970-01-01), "
                    + "and TIMESTAMP as logical 'timestamp-millis': written as long denoting milliseconds since Unix epoch. "
                    + "If a reader of written Avro records also knows these logical types, then these values can be deserialized with more context depending on reader implementation.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("qdbt-output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The maxvalue.* and fragment.count attributes will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor MAX_VALUE_COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Maximum-value Columns")
            .description("A comma-separated list of column names. The processor will keep track of the maximum value "
                    + "for each column that has been returned since the processor started running. Using multiple columns implies an order "
                    + "to the column list, and each column's values are expected to increase more slowly than the previous columns' values. Thus, "
                    + "using multiple columns implies a hierarchical structure of columns, which is usually used for partitioning tables. This processor "
                    + "can be used to retrieve only those rows that have been added/updated since the last retrieval. Note that some "
                    + "JDBC types such as bit/boolean are not conducive to maintaining maximum value, so columns of these "
                    + "types should not be listed in this property, and will result in error(s) during processing. If no columns "
                    + "are provided, all rows from the table will be considered, which could have a performance impact. NOTE: It is important "
                    + "to use consistent max-value column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    public static final String INITIAL_MAX_VALUE_PROP_START = "initial.maxvalue.";
    protected Map<String,String> maxValueProperties;
    protected static final String NAMESPACE_DELIMITER = "@!@";
    protected final Map<String, Integer> columnTypeMap = new HashMap<>();
    protected final AtomicBoolean setupComplete = new AtomicBoolean(false);
    private static SimpleDateFormat TIME_TYPE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        _propertyDescriptors.add(HIVEQL_PRE_QUERY);
        _propertyDescriptors.add(HIVEQL_SELECT_QUERY);
        _propertyDescriptors.add(HIVEQL_POST_QUERY);
        _propertyDescriptors.add(FETCH_SIZE);
        _propertyDescriptors.add(QUERY_TIMEOUT);
        _propertyDescriptors.add(MAX_ROWS_PER_FLOW_FILE);
        _propertyDescriptors.add(OUTPUT_BATCH_SIZE);
        _propertyDescriptors.add(MAX_VALUE_COLUMN_NAMES);
        _propertyDescriptors.add(MAX_FRAGMENTS);
        _propertyDescriptors.add(HIVEQL_OUTPUT_FORMAT);
        _propertyDescriptors.add(NORMALIZE_NAMES_FOR_AVRO);
        _propertyDescriptors.add(USE_AVRO_LOGICAL_TYPES);
        _propertyDescriptors.add(HIVEQL_CSV_HEADER);
        _propertyDescriptors.add(HIVEQL_CSV_ALT_HEADER);
        _propertyDescriptors.add(HIVEQL_CSV_DELIMITER);
        _propertyDescriptors.add(HIVEQL_CSV_QUOTE);
        _propertyDescriptors.add(HIVEQL_CSV_ESCAPE);
        _propertyDescriptors.add(CHARSET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        maxValueProperties = getDefaultMaxValueProperties(context, null);
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(HIVEQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
    }

    @OnStopped
    public void stop() {
        // Reset the column type map in case properties change
        setupComplete.set(false);
    }

    private static String apendWhereIntoSql(String sql,String appendWhere)
            throws JSQLParserException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Select select = (Select) parserManager.parse(new StringReader(sql));
        PlainSelect plain = (PlainSelect) select.getSelectBody();
        Expression where_expression = plain.getWhere();
        if(where_expression!=null) {
            Expression expression = new Parenthesis(where_expression);
            Expression appendExpression = CCJSqlParserUtil.parseCondExpression(appendWhere);
            Expression parenthesis = new Parenthesis(appendExpression);
            Expression newWhere = new AndExpression(parenthesis, expression);
            plain.setWhere(newWhere);
        }else{
            Expression appendExpression = CCJSqlParserUtil.parseCondExpression(appendWhere);
            Expression parenthesis = new Parenthesis(appendExpression);
            plain.setWhere(parenthesis);
        }
        return plain.toString();
    }

    public void setup(final ProcessContext context, boolean shouldCleanCache, FlowFile flowFile) {
        synchronized (setupComplete) {
            setupComplete.set(false);
            final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions(flowFile).getValue();

            // If there are no max-value column names specified, we don't need to perform this processing
            if (StringUtils.isEmpty(maxValueColumnNames)) {
                setupComplete.set(true);
                return;
            }
            List<String> preQueries = getQueries(context.getProperty(HIVEQL_PRE_QUERY).evaluateAttributeExpressions(flowFile).getValue());
            // Try to fill the columnTypeMap with the types of the desired max-value columns
            final Hive3DBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(Hive3DBCPService.class);
            try (final Connection con = dbcpService.getConnection(flowFile == null ? Collections.emptyMap() : flowFile.getAttributes());
                 final Statement st = con.createStatement()) {
                Pair<String,SQLException> failure = executeConfigStatements(con, preQueries);
                if (failure != null) {
                    // In case of failure, assigning config query to "hqlStatement"  to follow current error handling
                    throw failure.getRight();
                }
                String hqlStatement=context.getProperty(HIVEQL_SELECT_QUERY).evaluateAttributeExpressions(flowFile).getValue();
                String prefix=getMD5(hqlStatement);
                String query = apendWhereIntoSql(hqlStatement, "1 = 0");
                ResultSet resultSet = st.executeQuery(query);
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int numCols = resultSetMetaData.getColumnCount();
                if (numCols > 0) {
                    if (shouldCleanCache) {
                        columnTypeMap.clear();
                    }

                    final List<String> maxValueColumnNameList = Arrays.asList(maxValueColumnNames.toLowerCase().split(","));
                    final List<String> maxValueQualifiedColumnNameList = new ArrayList<>();

                    for (String maxValueColumn:maxValueColumnNameList) {
                        String colKey = getStateKey(prefix, maxValueColumn.trim());
                        maxValueQualifiedColumnNameList.add(colKey);
                    }

                    for (int i = 1; i <= numCols; i++) {
                        String colName = resultSetMetaData.getColumnName(i).toLowerCase();
                        String colKey = getStateKey(prefix, colName);

                        //only include columns that are part of the maximum value tracking column list
                        if (!maxValueQualifiedColumnNameList.contains(colKey)) {
                            continue;
                        }

                        int colType = resultSetMetaData.getColumnType(i);
                        columnTypeMap.putIfAbsent(colKey, colType);
                    }

                    for (String maxValueColumn:maxValueColumnNameList) {
                        String colKey = getStateKey(prefix, maxValueColumn.trim().toLowerCase());
                        if (!columnTypeMap.containsKey(colKey)) {
                            throw new ProcessException("Column not found in the table/query specified: " + maxValueColumn);
                        }
                    }
                } else {
                    throw new ProcessException("No columns found in table from those specified: " + maxValueColumnNames);
                }

            } catch (SQLException e) {
                throw new ProcessException("Unable to communicate with database in order to determine column types", e);
            } catch (JSQLParserException e) {
                throw new ProcessException("Unable to communicate with database in order to determine column types", e);
            }
            setupComplete.set(true);
        }
    }

    /**
     * Returns a SQL literal for the given value based on its type. For example, values of character type need to be enclosed
     * in single quotes, whereas values of numeric type should not be.
     *
     * @param type  The JDBC type for the desired literal
     * @param value The value to be converted to a SQL literal
     * @return A String representing the given value as a literal of the given type
     */
    private static String getLiteralByType(int type, String value) {
        switch (type) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
                return "'" + value + "'";
            case DATE:
            case TIMESTAMP:
                return "'" + value + "'";
            default:
                return value;
        }
    }

    protected String getQuery(String profix,String sqlQuery,List<String> maxValColumnNames, Map<String, String> stateMap) {
        if (StringUtils.isEmpty(profix)) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        List<String> whereClauses = new ArrayList<>();
        // Check state map for last max values
        if (stateMap != null && !stateMap.isEmpty() && maxValColumnNames != null) {
            IntStream.range(0, maxValColumnNames.size()).forEach((index) -> {
                String colName = maxValColumnNames.get(index);
                String maxValueKey = getStateKey(profix, colName);
                String maxValue = stateMap.get(maxValueKey);
                if (StringUtils.isEmpty(maxValue)) {
                    // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                    // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                    // maximum value is observed, it will be stored under the fully-qualified key from then on.
                    maxValue = stateMap.get(colName.toLowerCase());
                }
                if (!StringUtils.isEmpty(maxValue)) {
                    Integer type = columnTypeMap.get(maxValueKey);
                    if (type == null) {
                        // This shouldn't happen as we are populating columnTypeMap when the processor is scheduled.
                        throw new IllegalArgumentException("No column type found for: " + colName);
                    }
                    // Add a condition for the WHERE clause
                    whereClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue));
                }
            });
        }

        if (!whereClauses.isEmpty()) {
            String appendWhere = StringUtils.join(whereClauses, " AND ");
            try {
                String newSql = apendWhereIntoSql(sqlQuery, appendWhere);
                return newSql;
            } catch (JSQLParserException e) {
                throw new ProcessException("Unable to communicate with database in order to determine column types", e);
            }
        }
        return sqlQuery;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        PartialFunctions.onTrigger(context, sessionFactory, getLogger(), session -> onTrigger(context, session));
    }

    private void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = (context.hasIncomingConnection() ? session.get() : null);
        FlowFile flowfile = null;
        if (!setupComplete.get()) {
            this.setup(context,true,null);
        }

        // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
        // However, if we have no FlowFile and we have connections coming from other Processors, then
        // we know that we should run only if we have a FlowFile.
        if (context.hasIncomingConnection()) {
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();
        final Hive3DBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(Hive3DBCPService.class);
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());

        List<String> preQueries = getQueries(context.getProperty(HIVEQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
        List<String> postQueries = getQueries(context.getProperty(HIVEQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());

        final boolean flowbased = !(context.getProperty(HIVEQL_SELECT_QUERY).isSet());

        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();

        // Source the SQL
        String hqlStatement;

        if (context.getProperty(HIVEQL_SELECT_QUERY).isSet()) {
            hqlStatement = context.getProperty(HIVEQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, charset)));
            hqlStatement = queryContents.toString();
        }
        String prefix=getMD5(hqlStatement);
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions(fileToProcess).asInteger()
                : 0;
        final String outputFormat = context.getProperty(HIVEQL_OUTPUT_FORMAT).getValue();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final StopWatch stopWatch = new StopWatch(true);
        final boolean header = context.getProperty(HIVEQL_CSV_HEADER).asBoolean();
        final String altHeader = context.getProperty(HIVEQL_CSV_ALT_HEADER).evaluateAttributeExpressions(fileToProcess).getValue();
        final String delimiter = context.getProperty(HIVEQL_CSV_DELIMITER).evaluateAttributeExpressions(fileToProcess).getValue();
        final boolean quote = context.getProperty(HIVEQL_CSV_QUOTE).asBoolean();
        final boolean escape = context.getProperty(HIVEQL_CSV_HEADER).asBoolean();
        final boolean useLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final String fragmentIdentifier = UUID.randomUUID().toString();


        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }
        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        //If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
        for (final Map.Entry<String, String> maxProp : maxValueProperties.entrySet()) {
            String maxPropKey = maxProp.getKey().toLowerCase();
            String fullyQualifiedMaxPropKey = getStateKey(prefix, maxPropKey);
            if (!statePropertyMap.containsKey(fullyQualifiedMaxPropKey)) {
                String newMaxPropValue;
                // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                // the value has been stored under a key that is only the column name. Fall back to check the column name,
                // but store the new initial max value under the fully-qualified key.
                if (statePropertyMap.containsKey(maxPropKey)) {
                    newMaxPropValue = statePropertyMap.get(maxPropKey);
                } else {
                    newMaxPropValue = maxProp.getValue();
                }
                statePropertyMap.put(fullyQualifiedMaxPropKey, newMaxPropValue);

            }
        }
        List<String> maxValueColumnNameList = StringUtils.isEmpty(maxValueColumnNames)
                ? null
                : Arrays.asList(maxValueColumnNames.split("\\s*,\\s*"));
        String selectQuery = getQuery(prefix, hqlStatement, maxValueColumnNameList, statePropertyMap);
        this.getLogger().info("执行sql->"+selectQuery);
        try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
             final Statement st = (flowbased ? con.prepareStatement(selectQuery) : con.createStatement())
        ) {
            // Max values will be updated in the state property map by the callback
            final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(prefix, statePropertyMap);
            Pair<String,SQLException> failure = executeConfigStatements(con, preQueries);
            if (failure != null) {
                // In case of failure, assigning config query to "hqlStatement"  to follow current error handling
                selectQuery = failure.getLeft();
                flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                fileToProcess = null;
                throw failure.getRight();
            }
            st.setQueryTimeout(context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asInteger());

            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }

            final List<FlowFile> resultSetFlowFiles = new ArrayList<>();
            try {
                logger.debug("Executing query {}", new Object[]{selectQuery});
                if (flowbased) {
                    // Hive JDBC Doesn't Support this yet:
                    // ParameterMetaData pmd = ((PreparedStatement)st).getParameterMetaData();
                    // int paramCount = pmd.getParameterCount();

                    // Alternate way to determine number of params in SQL.
                    int paramCount = StringUtils.countMatches(selectQuery, "?");

                    if (paramCount > 0) {
                        setParameters(1, (PreparedStatement) st, paramCount, fileToProcess.getAttributes());
                    }
                }

                final ResultSet resultSet;

                try {
                    resultSet = (flowbased ? ((PreparedStatement) st).executeQuery() : st.executeQuery(selectQuery));
                } catch (SQLException se) {
                    // If an error occurs during the query, a flowfile is expected to be routed to failure, so ensure one here
                    flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                    fileToProcess = null;
                    throw se;
                }

                int fragmentIndex = 0;
                String baseFilename = (fileToProcess != null) ? fileToProcess.getAttribute(CoreAttributes.FILENAME.key()) : null;
                while (true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);
                    flowfile = (fileToProcess == null) ? session.create() : session.create(fileToProcess);
                    if (baseFilename == null) {
                        baseFilename = flowfile.getAttribute(CoreAttributes.FILENAME.key());
                    }
                    try {
                        flowfile = session.write(flowfile, out -> {
                            try {
                                if (AVRO.equals(outputFormat)) {
                                    nrOfRows.set(HiveJdbcCommon.convertToAvroStream(resultSet, out, null,maxRowsPerFlowFile, convertNamesForAvro,maxValCollector,useLogicalTypes));
                                } else if (CSV.equals(outputFormat)) {
                                    CsvOutputOptions options = new CsvOutputOptions(header, altHeader, delimiter, quote, escape, maxRowsPerFlowFile);
                                    nrOfRows.set(HiveJdbcCommon.convertToCsvStream(resultSet, out,null,maxValCollector, options));
                                } else {
                                    nrOfRows.set(0L);
                                    throw new ProcessException("Unsupported output format: " + outputFormat);
                                }
                            } catch (final SQLException | RuntimeException e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(flowfile);
                        throw e;
                    }

                    if (nrOfRows.get() > 0 || resultSetFlowFiles.isEmpty()) {
                        final Map<String, String> attributes = new HashMap<>();
                        // Set attribute for how many rows were selected
                        attributes.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));

                        try {
                            // Set input/output table names by parsing the query
                            attributes.putAll(toQueryTableAttributes(findTableNames(selectQuery)));
                        } catch (Exception e) {
                            // If failed to parse the query, just log a warning message, but continue.
                            getLogger().warn("Failed to parse query: {} due to {}", new Object[]{selectQuery, e}, e);
                        }

                        // Set MIME type on output document and add extension to filename
                        if (AVRO.equals(outputFormat)) {
                            attributes.put(CoreAttributes.MIME_TYPE.key(), MIME_TYPE_AVRO_BINARY);
                            attributes.put(CoreAttributes.FILENAME.key(), baseFilename + "." + fragmentIndex + ".avro");
                        } else if (CSV.equals(outputFormat)) {
                            attributes.put(CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);
                            attributes.put(CoreAttributes.FILENAME.key(), baseFilename + "." + fragmentIndex + ".csv");
                        }

                        if (maxRowsPerFlowFile > 0) {
                            attributes.put("fragment.identifier", fragmentIdentifier);
                            attributes.put("fragment.index", String.valueOf(fragmentIndex));
                        }

                        flowfile = session.putAllAttributes(flowfile, attributes);

                        logger.info("{} contains {} " + outputFormat + " records; transferring to 'success'",
                                new Object[]{flowfile, nrOfRows.get()});

                        if (context.hasIncomingConnection()) {
                            // If the flow file came from an incoming connection, issue a Fetch provenance event
                            session.getProvenanceReporter().fetch(flowfile, dbcpService.getConnectionURL(),
                                    "Retrieved " + nrOfRows.get() + " rows", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        } else {
                            // If we created a flow file from rows received from Hive, issue a Receive provenance event
                            session.getProvenanceReporter().receive(flowfile, dbcpService.getConnectionURL(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        }
                        resultSetFlowFiles.add(flowfile);
                        // If we've reached the batch size, send out the flow files
                        if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                            session.adjustCounter("read sizes", resultSetFlowFiles.size(), false);//ldp20200416
                            session.transfer(resultSetFlowFiles, REL_SUCCESS);
                            session.commit();
                            resultSetFlowFiles.clear();
                        }
                    } else {
                        // If there were no rows returned (and the first flow file has been sent, we're done processing, so remove the flowfile and carry on
                        session.remove(flowfile);
                        if (resultSetFlowFiles != null && resultSetFlowFiles.size()>0) {
                            flowfile = resultSetFlowFiles.get(resultSetFlowFiles.size()-1);
                        }
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }
                }

                for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                    // Set count on all FlowFiles
                    if (maxRowsPerFlowFile > 0) {
                        resultSetFlowFiles.set(i,
                                session.putAttribute(resultSetFlowFiles.get(i), "fragment.count", Integer.toString(fragmentIndex)));
                    }
                }

            } catch (final SQLException e) {
                throw e;
            }

            // Apply state changes from the Max Value tracker
            maxValCollector.applyStateChanges();

            failure = executeConfigStatements(con, postQueries);
            if (failure != null) {
                selectQuery = failure.getLeft();
                if (resultSetFlowFiles != null) {
                    resultSetFlowFiles.forEach(ff -> session.remove(ff));
                }
                flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                fileToProcess = null;
                throw failure.getRight();
            }

            session.adjustCounter("read sizes", resultSetFlowFiles.size(), false);//ldp20200416
            session.transfer(resultSetFlowFiles, REL_SUCCESS);
            if (fileToProcess != null) {
                session.remove(fileToProcess);
            }
        } catch (final ProcessException | SQLException e) {
            logger.error("Issue processing SQL {} due to {}.", new Object[]{selectQuery, e});
            if (flowfile == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute HiveQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute HiveQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, flowfile, e});
                    flowfile = session.penalize(flowfile);
                } else {
                    logger.error("Unable to execute HiveQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.transfer(flowfile, REL_FAILURE);
            }
        } finally {
            session.commit();
            try {
                // Update the state
                stateManager.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", new Object[]{this, ioe});
            }
        }
    }

    /*
     * Executes given queries using pre-defined connection.
     * Returns null on success, or a query string if failed.
     */
    protected Pair<String,SQLException> executeConfigStatements(final Connection con, final List<String> configQueries){
        if (configQueries == null || configQueries.isEmpty()) {
            return null;
        }

        for (String confSQL : configQueries) {
            try(final Statement st = con.createStatement()){
                st.execute(confSQL);
            } catch (SQLException e) {
                return Pair.of(confSQL, e);
            }
        }
        return null;
    }

    protected List<String> getQueries(final String value) {
        if (value == null || value.length() == 0 || value.trim().length() == 0) {
            return null;
        }
        final List<String> queries = new LinkedList<>();
        for (String query : value.split(";")) {
            if (query.trim().length() > 0) {
                queries.add(query.trim());
            }
        }
        return queries;
    }

    private Map<String, String> getDefaultMaxValueProperties(final ProcessContext context, final FlowFile flowFile) {
        final Map<String, String> defaultMaxValues = new HashMap<>();

        context.getProperties().forEach((k, v) -> {
            final String key = k.getName();

            if (key.startsWith(INITIAL_MAX_VALUE_PROP_START)) {
                defaultMaxValues.put(key.substring(INITIAL_MAX_VALUE_PROP_START.length()), context.getProperty(k).evaluateAttributeExpressions(flowFile).getValue());
            }
        });
        return defaultMaxValues;
    }

    /**
     * Construct a key string for a corresponding state value.
     * @param prefix A prefix may contain database and table name, or just table name, this can be null
     * @param columnName A column name
     * @return a state key string
     */
    private static String getStateKey(String prefix, String columnName) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(unwrapIdentifier(prefix.toLowerCase()));
            sb.append(NAMESPACE_DELIMITER);
        }
        if (columnName != null) {
            sb.append(unwrapIdentifier(columnName.toLowerCase()));
        }
        return sb.toString();
    }

    public static String unwrapIdentifier(String identifier) {
        // Removes double quotes and back-ticks.
        return identifier == null ? null : identifier.replaceAll("[\"`]", "");
    }

    /**
     * 对字符串md5加密(小写+字母)
     *
     * @param str 传入要加密的字符串
     * @return  MD5加密后的字符串
     */
    public static String getMD5(String str) {
        try {
            // 生成一个MD5加密计算摘要
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 计算md5函数
            md.update(str.getBytes());
            // digest()最后确定返回md5 hash值，返回值为8为字符串。因为md5 hash值是16位的hex值，实际上就是8位的字符
            // BigInteger函数则将8位的字符串转换成16位hex值，用字符串来表示；得到字符串形式的hash值
            return new BigInteger(1, md.digest()).toString(16);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    protected static String getMaxValueFromRow(ResultSet resultSet,
                                               int columnIndex,
                                               Integer type,
                                               String maxValueString)
            throws ParseException, IOException, SQLException {

        // Skip any columns we're not keeping track of or whose value is null
        if (type == null || resultSet.getObject(columnIndex) == null) {
            return null;
        }

        switch (type) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
                String colStringValue = resultSet.getString(columnIndex);
                if (maxValueString == null || colStringValue.compareTo(maxValueString) > 0) {
                    return colStringValue;
                }
                break;

            case INTEGER:
            case SMALLINT:
            case TINYINT:
                Integer colIntValue = resultSet.getInt(columnIndex);
                Integer maxIntValue = null;
                if (maxValueString != null) {
                    maxIntValue = Integer.valueOf(maxValueString);
                }
                if (maxIntValue == null || colIntValue > maxIntValue) {
                    return colIntValue.toString();
                }
                break;

            case BIGINT:
                Long colLongValue = resultSet.getLong(columnIndex);
                Long maxLongValue = null;
                if (maxValueString != null) {
                    maxLongValue = Long.valueOf(maxValueString);
                }
                if (maxLongValue == null || colLongValue > maxLongValue) {
                    return colLongValue.toString();
                }
                break;

            case FLOAT:
            case REAL:
            case DOUBLE:
                Double colDoubleValue = resultSet.getDouble(columnIndex);
                Double maxDoubleValue = null;
                if (maxValueString != null) {
                    maxDoubleValue = Double.valueOf(maxValueString);
                }
                if (maxDoubleValue == null || colDoubleValue > maxDoubleValue) {
                    return colDoubleValue.toString();
                }
                break;

            case DECIMAL:
            case NUMERIC:
                BigDecimal colBigDecimalValue = resultSet.getBigDecimal(columnIndex);
                BigDecimal maxBigDecimalValue = null;
                if (maxValueString != null) {
                    DecimalFormat df = new DecimalFormat();
                    df.setParseBigDecimal(true);
                    maxBigDecimalValue = (BigDecimal) df.parse(maxValueString);
                }
                if (maxBigDecimalValue == null || colBigDecimalValue.compareTo(maxBigDecimalValue) > 0) {
                    return colBigDecimalValue.toString();
                }
                break;

            case DATE:
                Date rawColDateValue = resultSet.getDate(columnIndex);
                java.sql.Date colDateValue = new java.sql.Date(rawColDateValue.getTime());
                java.sql.Date maxDateValue = null;
                if (maxValueString != null) {
                    maxDateValue = java.sql.Date.valueOf(maxValueString);
                }
                if (maxDateValue == null || colDateValue.after(maxDateValue)) {
                    return colDateValue.toString();
                }
                break;

            case TIME:
                // Compare milliseconds-since-epoch. Need getTimestamp() instead of getTime() since some databases
                // don't return milliseconds in the Time returned by getTime().
                Date colTimeValue = new Date(resultSet.getTimestamp(columnIndex).getTime());
                Date maxTimeValue = null;
                if (maxValueString != null) {
                    try {
                        maxTimeValue = TIME_TYPE_FORMAT.parse(maxValueString);
                    } catch (ParseException pe) {
                        // Shouldn't happen, but just in case, leave the value as null so the new value will be stored
                    }
                }
                if (maxTimeValue == null || colTimeValue.after(maxTimeValue)) {
                    return TIME_TYPE_FORMAT.format(colTimeValue);
                }
                break;

            case TIMESTAMP:
                Timestamp colTimestampValue = resultSet.getTimestamp(columnIndex);
                java.sql.Timestamp maxTimestampValue = null;
                if (maxValueString != null) {
                    // For backwards compatibility, the type might be TIMESTAMP but the state value is in DATE format. This should be a one-time occurrence as the next maximum value
                    // should be stored as a full timestamp. Even so, check to see if the value is missing time-of-day information, and use the "date" coercion rather than the
                    // "timestamp" coercion in that case
                    try {
                        maxTimestampValue = java.sql.Timestamp.valueOf(maxValueString);
                    } catch (IllegalArgumentException iae) {
                        maxTimestampValue = new java.sql.Timestamp(java.sql.Date.valueOf(maxValueString).getTime());
                    }
                }
                if (maxTimestampValue == null || colTimestampValue.after(maxTimestampValue)) {
                    return colTimestampValue.toString();
                }
                break;

            case BIT:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case ARRAY:
            case BLOB:
            case CLOB:
            default:
                throw new IOException("Type for column " + columnIndex + " is not valid for maintaining maximum value");
        }
        return null;
    }

    public class MaxValueResultSetRowCollector implements HiveJdbcCommon.ResultSetRowCallback {
        final Map<String, String> newColMap;
        final Map<String, String> originalState;
        String tableName;

        public MaxValueResultSetRowCollector(String tableName, Map<String, String> stateMap) {
            this.originalState = stateMap;
            this.newColMap = new HashMap<>();
            this.newColMap.putAll(stateMap);
            this.tableName = tableName;
        }

        @Override
        public void processRow(ResultSet resultSet) throws IOException {
            if (resultSet == null) {
                return;
            }
            try {
                // Iterate over the row, check-and-set max values
                final ResultSetMetaData meta = resultSet.getMetaData();
                final int nrOfColumns = meta.getColumnCount();
                if (nrOfColumns > 0) {
                    for (int i = 1; i <= nrOfColumns; i++) {
                        String colName = meta.getColumnName(i).toLowerCase();
                        String fullyQualifiedMaxValueKey = getStateKey(tableName, colName);
                        Integer type = columnTypeMap.get(fullyQualifiedMaxValueKey);
                        // Skip any columns we're not keeping track of or whose value is null
                        if (type == null || resultSet.getObject(i) == null) {
                            continue;
                        }
                        String maxValueString = newColMap.get(fullyQualifiedMaxValueKey);
                        // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                        // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                        // maximum value is observed, it will be stored under the fully-qualified key from then on.
                        if (StringUtils.isEmpty(maxValueString)) {
                            maxValueString = newColMap.get(colName);
                        }
                        String newMaxValueString = getMaxValueFromRow(resultSet, i, type, maxValueString);
                        if (newMaxValueString != null) {
                            newColMap.put(fullyQualifiedMaxValueKey, newMaxValueString);
                        }
                    }
                }
            } catch (ParseException | SQLException e) {
                throw new IOException(e);
            }
        }

        public void applyStateChanges() {
            this.originalState.putAll(this.newColMap);
        }
    }
}