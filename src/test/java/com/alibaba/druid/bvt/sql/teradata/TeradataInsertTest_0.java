package com.alibaba.druid.bvt.sql.teradata;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.junit.Assert;

import com.alibaba.druid.sql.TeradataTest;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleInsertStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataInsertStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelectQueryBlock;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.util.Utils;

public class TeradataInsertTest_0 extends TeradataTest {
	
	public void test_0() throws Exception {
//		exec_test("bvt/parser/teradata-ins-0.txt");
		exec_test("bvt/parser/teradata-ins-0.txt");
//		exec_test_mysql("bvt/parser/teradata-ins-1.txt");
//		exec_test_oracle("bvt/parser/teradata-ins-2.txt");
	}
	
	public void exec_test(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String ins_sql = input.trim();
		
		TeradataStatementParser parser = new TeradataStatementParser(ins_sql);
		List<SQLStatement> statementList = parser.parseStatementList();
		SQLStatement stmt = statementList.get(0);
		
		Assert.assertTrue(stmt instanceof SQLInsertStatement);
			
		TeradataInsertStatement insertStmt = (TeradataInsertStatement) stmt;
		System.out.println(insertStmt);
		SQLSelect insertQuery = insertStmt.getQuery();
		Assert.assertNotNull(insertQuery);
		
		// retrieve SELECT sub-clause from INSERT query
		TeradataSelectQueryBlock insertBlock = (TeradataSelectQueryBlock) insertQuery.getQuery();
		Assert.assertNotNull(insertBlock);
		
		// target table
//		Assert.assertEquals("d1.t1", insertStmt.getTableSource().toString());
//		Assert.assertEquals(2, insertStmt.getColumns().size());
//		Assert.assertEquals("col1", insertStmt.getColumns().get(0).toString());
//		Assert.assertEquals("col2", insertStmt.getColumns().get(1).toString());
//		// source table
//		Assert.assertEquals("d2.t2", insertBlock.getFrom().toString());
//		Assert.assertEquals(2, insertBlock.getSelectList().size());
//		Assert.assertEquals("c1", insertBlock.getSelectList().get(0).toString());
//		Assert.assertEquals("c2", insertBlock.getSelectList().get(1).toString());
        
        System.out.println("***********below is useful info **********");
        System.out.println("target table: " + insertStmt.getTableSource());
        System.out.println("target columns: " + insertStmt.getColumns());
        System.out.println("select block: " + insertBlock);
        System.out.println("source from: " + insertBlock.getFrom());
        TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
        insertBlock.getFrom().accept(fromVisitor);
        
        if (fromVisitor.getAliasQueryMap().containsKey("col2".toLowerCase())) {
        	
        	System.out.println("sub query: " + fromVisitor.getAliasQuery("col2"));
           
        }
        
        System.out.println("source columns : " + insertBlock.getSelectList());
        System.out.println("source condition: " + insertBlock.getWhere());
        System.out.println("********");
		
        TeradataSchemaStatVisitor visitor = new TeradataSchemaStatVisitor();
//        stmt.accept(visitor);
        insertBlock.getSelectList().get(0).getExpr().accept(visitor);
        System.out.println("alias map: " + visitor.getAliasMap());
        
//        System.out.println("sub query map: " + visitor.getSubQueryMap());
//        SQLSelect select = (SQLSelect) visitor.getSubQueryMap().get("five");
//        TeradataSchemaStatVisitor visitor1 = new TeradataSchemaStatVisitor();
//        select.accept(visitor1);
        
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
        for (Column column : visitor.getColumns()) {
        	System.out.println(column.toString());
        }
        System.out.println("coditions : " + visitor.getConditions());
        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        System.out.println("------ end of " + new Object(){}.getClass().getEnclosingMethod().getName() + " ------");

	}
	
	
	public void exec_test_mysql(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String ins_sql = input.trim();
		
		MySqlStatementParser parser = new MySqlStatementParser(ins_sql);
		List<SQLStatement> statementList = parser.parseStatementList();
		SQLStatement stmt = statementList.get(0);
		
		Assert.assertTrue(stmt instanceof SQLInsertStatement);
			
		MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;
		SQLSelect insertQuery = insertStmt.getQuery();
		Assert.assertNotNull(insertQuery);
		
		// retrieve SELECT sub-clause from INSERT query
		MySqlSelectQueryBlock insertBlock = (MySqlSelectQueryBlock) insertQuery.getQuery();
		Assert.assertNotNull(insertBlock);
        
        System.out.println("***********below is useful info **********");
        System.out.println("target table: " + insertStmt.getTableSource());
        System.out.println("target columns: " + insertStmt.getColumns());
        System.out.println("source from: " + insertBlock.getFrom());
        System.out.println("source columns : " + insertBlock.getSelectList());
        System.out.println("source condition: " + insertBlock.getWhere());
        System.out.println("********");
		
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        
        stmt.accept(visitor);
        
        System.out.println(visitor.getAliasMap());
        
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
        System.out.println("coditions : " + visitor.getConditions());
        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        System.out.println("------ end of " + new Object(){}.getClass().getEnclosingMethod().getName() + " ------");

	}
	
	public void exec_test_oracle(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String ins_sql = input.trim();
		
		OracleStatementParser parser = new OracleStatementParser(ins_sql);
		List<SQLStatement> statementList = parser.parseStatementList();
		SQLStatement stmt = statementList.get(0);
		
		Assert.assertTrue(stmt instanceof SQLInsertStatement);
			
		OracleInsertStatement insertStmt = (OracleInsertStatement) stmt;
		SQLSelect insertQuery = insertStmt.getQuery();
		Assert.assertNotNull(insertQuery);
		
		// retrieve SELECT sub-clause from INSERT query
		OracleSelectQueryBlock insertBlock = (OracleSelectQueryBlock) insertQuery.getQuery();
		Assert.assertNotNull(insertBlock);
        
        System.out.println("***********below is useful info **********");
        System.out.println("target table: " + insertStmt.getTableSource());
        System.out.println("target columns: " + insertStmt.getColumns());
        System.out.println("source from: " + insertBlock.getFrom());
        System.out.println("source columns : " + insertBlock.getSelectList());
        System.out.println("source condition: " + insertBlock.getWhere());
        System.out.println("********");
		
        OracleSchemaStatVisitor visitor = new OracleSchemaStatVisitor();
        
//        stmt.accept(visitor);
        insertBlock.getSelectList().get(0).getExpr().accept(visitor);
        
        System.out.println(visitor.getAliasMap());
        
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
        System.out.println("coditions : " + visitor.getConditions());
        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        System.out.println("------ end of " + new Object(){}.getClass().getEnclosingMethod().getName() + " ------");

	}
	
}
