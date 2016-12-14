/*
 * Copyright 1999-2101 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.bvt.sql.mysql;

import java.util.List;

import org.junit.Assert;

import com.alibaba.druid.sql.MysqlTest;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

public class MySqlTestVsGsp extends MysqlTest {

    public void test_0() throws Exception {
        // 1. select
//        String sql = "SELECT a1, a2 FROM t_department  WHERE name IN ('0000','4444') ORDER BY name ASC";
        String sql = "SELECT t1.a1, t2.a2 FROM t1 LEFT JOIN t2 ON t1.id = t2.id;";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        if(stmt instanceof SQLSelectStatement) {
            System.out.println("yes, is SELECT of mysql");
        } else {
            System.out.println("nop...");
        }
        
        System.out.println("select stmt: " + stmt);
        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        SQLSelect selectSQL = selectStmt.getSelect();
        
        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock)selectSQL.getQuery();
        System.out.println("select list: " + queryBlock.getSelectList());
        System.out.println("select item size: " + queryBlock.getSelectList().size());
        SQLSelectItem selectItem = queryBlock.getSelectList().get(0);
        System.out.println("select item: " + selectItem.getExpr().toString());
        
        SQLJoinTableSource joinSource = (SQLJoinTableSource)queryBlock.getFrom();
        System.out.println("join type: " + joinSource.getJoinType());
        System.out.println("join left: " + joinSource.getLeft());
        System.out.println("join right: " + joinSource.getRight());
        System.out.println("join condition: " + joinSource.getCondition());
//        System.out.println("select list: " + (SQLJoinTableSource)queryBlock.getParent());
//        System.out.println(queryBlock);
        
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        
        selectSQL.accept(visitor);
//        queryBlock.accept(visitor);
//        selectStmt.accept(visitor);
        System.out.println("relationships: " + visitor.getRelationships());
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getCurrentTable());
        
        // 2. insert
        System.out.println("***********************************");
//        String ins_sql = "insert into t1(col1, col2) values ('a', 'b'), ('c', 'd');";
        String ins_sql = "insert into d1.t1(col1, col2) select c1, c2 from d2.t2 where c1=0;";
        parser = new MySqlStatementParser(ins_sql);
        SQLStatement ins_stmt = parser.parseStatementList().get(0);
        if(ins_stmt instanceof SQLInsertStatement) {
            System.out.println("yes, is INSERT of mysql");
        } else {
            System.out.println("nop...");
        }
        
        System.out.println(ins_stmt);
        MySqlInsertStatement insertStmt = (MySqlInsertStatement) ins_stmt;
        System.out.println("table: " + insertStmt.getTableName());
//        System.out.println("target table: " + insertStmt.getTableSource());
//        System.out.println("columns: " + insertStmt.getColumns());
        System.out.println("values: " + insertStmt.getValuesList());
        
        
        SQLSelect insertQuery = insertStmt.getQuery();
        MySqlSelectQueryBlock insertBlock = (MySqlSelectQueryBlock) insertQuery.getQuery();
        System.out.println("select list : " + insertBlock.getSelectList());
        
        System.out.println("***********below is needed info**********");
        System.out.println("target table: " + insertStmt.getTableSource());
        System.out.println("target columns: " + insertStmt.getColumns());
        System.out.println("source table: " + insertBlock.getFrom());
        System.out.println("source columns : " + insertBlock.getSelectList());
        System.out.println("source condition: " + insertBlock.getWhere());
        System.out.println("*****************************************");
        
        visitor = new MySqlSchemaStatVisitor();
        ins_stmt.accept(visitor);
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
        System.out.println("coditions : " + visitor.getConditions());
        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        // 3. update
        System.out.println("***********************************");
        String upd_sql = "UPDATE z_code SET key='INTRANT_NOTALLOWED_CATEGORY_C' where c1 = 0;";

        parser = new MySqlStatementParser(upd_sql);
        SQLStatement upd_stmt = parser.parseStatementList().get(0);
        if(upd_stmt instanceof SQLUpdateStatement) {
            System.out.println("yes, is UPDATE of mysql");
        } else {
            System.out.println("nop...");
        }
        
        System.out.println(upd_stmt);
        MySqlUpdateStatement updateStmt = (MySqlUpdateStatement) upd_stmt;
        System.out.println("table name: "+ updateStmt.getTableName());
        System.out.println("table source: "+ updateStmt.getTableSource());
        System.out.println("set item value: "+ updateStmt.getItems().get(0));
        System.out.println("where cond: "+ updateStmt.getWhere());
        
        visitor = new MySqlSchemaStatVisitor();
        upd_stmt.accept(visitor);

        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
        System.out.println("coditions : " + visitor.getConditions());
        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        // 4. delete
        System.out.println("***********************************");
        String del_sql = "DELETE FROM t1 " //
                + "WHERE s11 > ANY"//
                + "(SELECT COUNT(*) /* no hint */ FROM t2"//
                + "  WHERE NOT EXISTS"//
                + "   (SELECT * FROM t3"//
                + "    WHERE ROW(5*t2.s1,77)="//
                + "     (SELECT 50,11*s1 FROM t4 UNION SELECT 50,77 FROM"//
                + "      (SELECT * FROM t5) AS t5)));";
        parser = new MySqlStatementParser(del_sql);
        SQLStatement del_stmt = parser.parseStatementList().get(0);
        if(del_stmt instanceof SQLDeleteStatement) {
            System.out.println("yes, is DELETE of mysql");
        } else {
            System.out.println("nop...");
        }
        System.out.println(del_stmt);
        MySqlDeleteStatement deleteStmt = (MySqlDeleteStatement)del_stmt;
        System.out.println("table name: "+ deleteStmt.getTableName());
        System.out.println("table source: "+ deleteStmt.getTableSource());
        System.out.println("set item value: "+ deleteStmt.getExprTableSource());
        System.out.println("where cond: "+ deleteStmt.getWhere());
        
    }
}
