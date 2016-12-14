package com.alibaba.druid.bvt.sql.teradata;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.junit.Assert;

import com.alibaba.druid.sql.TeradataTest;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleCreateTableStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleUpdateStatement;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.postgresql.ast.stmt.PGUpdateStatement;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGSQLStatementParser;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataCreateTableStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataUpdateStatement;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.util.Utils;

public class TeradataUpdateTest_0 extends TeradataTest {
	
	public void test_0() throws Exception {
		exec_test_td("bvt/parser/teradata-upd-2.txt");
//		exec_column("bvt/parser/teradata-upd-1.txt");
//		exec_select("bvt/parser/teradata-upd-1.txt");

//		exec_test_oracle("bvt/parser/teradata-upd-1.txt");
//		exec_test_mysql("bvt/parser/teradata-upd-1.txt");
//		exec_test_pg("bvt/parser/teradata-upd-1.txt");

	}
	
    public void exec_column(String resource) throws Exception {
    	System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String cre_sql = input.trim();

    	TeradataStatementParser parser = new TeradataStatementParser(cre_sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        
        SQLStatement statement = statementList.get(0);
        
        Assert.assertTrue(statement instanceof TeradataCreateTableStatement);
        
        TeradataCreateTableStatement createStmt = (TeradataCreateTableStatement) statement;
        
        Assert.assertEquals("table source", createStmt.getTableSource().toString().toLowerCase(), "dw_ps_occur_count_v");
        Assert.assertNull(createStmt.getSelect());
    }
    
    /*
     * test for 
     *   td update...from...set..
     */
    public void exec_test_td(String resource) throws Exception {
    	System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String upd_sql = input.trim();

    	TeradataStatementParser parser = new TeradataStatementParser(upd_sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        
        SQLStatement statement = statementList.get(0);
        
        Assert.assertTrue(statement instanceof TeradataUpdateStatement);
        
        TeradataUpdateStatement updStmt = (TeradataUpdateStatement) statement;
               
        
//        SQLSelect select = createStmt.getSelect();
//        SQLSelectQueryBlock selectQuery = (SQLSelectQueryBlock) select.getQuery();
////        SQLSelectQueryBlock createBlock = (SQLSelectQueryBlock) selectQuery.getQuery(); 
//        
//        System.out.println("table created: " + createStmt.getTableSource());
//        System.out.println("select query: " + selectQuery);
////        System.out.println("create stmt: " + createStmt);
//        
        TeradataSchemaStatVisitor visitor = new TeradataSchemaStatVisitor();
        updStmt.accept(visitor);
//        
        System.out.println("columns: " + visitor.getColumns());
        System.out.println(visitor.getAliasMap());
        System.out.println(visitor.getAliasQueryMap());
        
    }
    
    public void exec_test_mysql(String resource) throws Exception {
    	System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String upd_sql = input.trim();

    	MySqlStatementParser parser = new MySqlStatementParser(upd_sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        
        SQLStatement statement = statementList.get(0);
        
        Assert.assertTrue(statement instanceof MySqlUpdateStatement);
        
        MySqlUpdateStatement updStmt = (MySqlUpdateStatement) statement;
        
        updStmt.getItems();
    }
    
    public void exec_test_oracle(String resource) throws Exception {
    	System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String upd_sql = input.trim();
		
		OracleStatementParser parser = new OracleStatementParser(upd_sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        
        SQLStatement statement = statementList.get(0);
        
        Assert.assertTrue(statement instanceof OracleUpdateStatement);
        
        OracleUpdateStatement updStmt = (OracleUpdateStatement) statement;
        
        updStmt.getAlias();
              
//        OracleSchemaStatVisitor visitor = new OracleSchemaStatVisitor();
//        statement.accept(visitor);
//        
//        visitor.getColumns();
    }
    
    public void exec_test_pg(String resource) throws Exception {
    	System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String upd_sql = input.trim();
		
		PGSQLStatementParser parser = new PGSQLStatementParser(upd_sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        
        SQLStatement statement = statementList.get(0);
        
        Assert.assertTrue(statement instanceof PGUpdateStatement);
        
        PGUpdateStatement updStmt = (PGUpdateStatement) statement;
        
        updStmt.getFrom();
//        OracleSchemaStatVisitor visitor = new OracleSchemaStatVisitor();
//        statement.accept(visitor);
//        
//        visitor.getColumns();
    }
}
