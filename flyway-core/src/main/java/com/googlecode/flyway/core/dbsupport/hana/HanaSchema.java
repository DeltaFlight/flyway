/**
 * Copyright 2010-2013 Axel Fontaine and the many contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.googlecode.flyway.core.dbsupport.hana;

import com.googlecode.flyway.core.dbsupport.DbSupport;
import com.googlecode.flyway.core.dbsupport.JdbcTemplate;
import com.googlecode.flyway.core.dbsupport.Schema;
import com.googlecode.flyway.core.dbsupport.Table;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hana implementation of Schema.
 */
public class HanaSchema extends Schema {
    /**
     * Creates a new Hana schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param dbSupport    The database-specific support.
     * @param name         The name of the schema.
     */
    public HanaSchema(JdbcTemplate jdbcTemplate, DbSupport dbSupport, String name) {
        super(jdbcTemplate, dbSupport, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM schemas WHERE schema_name=?", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        int objectCount = jdbcTemplate.queryForInt("Select "
                + "(select count(*) from SYS.VIEWS where schema_name=?) + "
                + "(select count(*) from SYS.M_RS_TABLES where schema_name=?) + "
                + "(select count(*) from SYS.PROCEDURES where schema_name=?) + "
                + "(select count(*) from SYS.FUNCTIONS where schema_name=?) + "
                + " FROM dummy",
                name, name, name, name);
        return objectCount == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + dbSupport.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP SCHEMA " + dbSupport.quote(name) + " CASCADE");
    }

    @Override
    protected void doClean() throws SQLException {
        for (String statement : cleanProcedures()) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : cleanFunctions()) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : cleanViews()) {
            jdbcTemplate.execute(statement);
        }

        jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0");
        for (Table table : allTables()) {
            table.drop();
        }
        jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1");
    }

    /**
     * Generate the statements to clean the procedures in this schema.
     *
     * @return The list of statements.
     * @throws java.sql.SQLException when the clean statements could not be generated.
     */
    private List<String> cleanProcedures() throws SQLException {
        List<String> names =
                jdbcTemplate.queryForStringList(
                        "SELECT procedure_name FROM SYS.PROCEDURES WHERE schema_name=?",
                        name);

        List<String> statements = new ArrayList<String>();
        for (String name : names) {
            statements.add("DROP PROCEDURE " + dbSupport.quote(name, name));
        }
        return statements;
    }

    /**
     * Generate the statements to clean the functions in this schema.
     *
     * @return The list of statements.
     * @throws java.sql.SQLException when the clean statements could not be generated.
     */
    private List<String> cleanFunctions() throws SQLException {
        List<String> names =
                jdbcTemplate.queryForStringList(
                        "SELECT function_name FROM SYS.FUNCTIONS WHERE schema_name=?",
                        name);

        List<String> statements = new ArrayList<String>();
        for (String name : names) {
            statements.add("DROP FUNCTION " + dbSupport.quote(name, name));
        }
        return statements;
    }

    /**
     * Generate the statements to clean the views in this schema.
     *
     * @return The list of statements.
     * @throws java.sql.SQLException when the clean statements could not be generated.
     */
    private List<String> cleanViews() throws SQLException {
        List<String> viewNames =
                jdbcTemplate.queryForStringList(
                        "select * from VIEWS where schema_name=?", name);

        List<String> statements = new ArrayList<String>();
        for (String viewName : viewNames) {
            statements.add("DROP VIEW " + dbSupport.quote(name, viewName));
        }
        return statements;
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(
                "select table_name from M_RS_TABLES where schema_name=?'", name);

        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new HanaTable(jdbcTemplate, dbSupport, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new HanaTable(jdbcTemplate, dbSupport, this, tableName);
    }
}
