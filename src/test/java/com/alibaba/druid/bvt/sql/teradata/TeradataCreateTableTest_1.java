package com.alibaba.druid.bvt.sql.teradata;

import java.util.List;

import org.junit.Assert;

import com.alibaba.druid.sql.TeradataTest;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

public class TeradataCreateTableTest_1 extends TeradataTest {
    public void test_0() {
    	String sql = "CREATE TABLE t AS\n" + //
                "SELECT ProductID,ProductName\n" + //
                "FROM Products\n" + //
                "WHERE Discontinued=No";
    	MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        
        SQLStatement statement = statementList.get(0);
        
        Assert.assertTrue(statement instanceof MySqlCreateTableStatement);
        
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) statement;
        
        SQLSelect select = createStmt.getSelect();
        SQLSelectQueryBlock selectQuery = (SQLSelectQueryBlock) select.getQuery();
//        SQLSelectQueryBlock createBlock = (SQLSelectQueryBlock) selectQuery.getQuery(); 
        
        System.out.println(createStmt.getTableSource());
        
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statement.accept(visitor);
        
        visitor.getColumns();
    }
}
