package com.alibaba.druid.bvt.sql.teradata;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.junit.Assert;

import com.alibaba.druid.sql.TeradataTest;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataDeleteStatement;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.util.Utils;

public class TeradataDeleteTest_0 extends TeradataTest {
	
	public void test_0() throws Exception {
//		exec_test_mysql("bvt/parser/teradata-del-1.txt");
		exec_test_td("bvt/parser/teradata-del-1.txt");
	} 
	
	private void exec_test_td(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String del_sql = input.trim();

		TeradataStatementParser parser = new TeradataStatementParser(del_sql);
		List<SQLStatement> statementList = parser.parseStatementList();
		
		SQLStatement statement = statementList.get(0);
		
		Assert.assertTrue(statement instanceof TeradataDeleteStatement);
		
		System.out.println(statement);
		
	}
	
	private void exec_test_mysql(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String del_sql = input.trim();

    	MySqlStatementParser parser = new MySqlStatementParser(del_sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        
        SQLStatement statement = statementList.get(0);
        
        Assert.assertTrue(statement instanceof MySqlDeleteStatement);
        
	}
}
