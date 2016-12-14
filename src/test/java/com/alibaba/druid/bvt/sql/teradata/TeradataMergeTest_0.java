package com.alibaba.druid.bvt.sql.teradata;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.junit.Assert;

import com.alibaba.druid.sql.TeradataTest;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleMergeStatement;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataMergeStatement;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.util.Utils;

public class TeradataMergeTest_0 extends TeradataTest {
	
	public void test_0() throws Exception {
//		exec_test_oracle("bvt/parser/teradata-mrg-1.txt");
		exec_test_td("bvt/parser/teradata-mrg-1.txt");
		
	}
	
	private void exec_test_td(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String mrg_sql = input.trim();
		
		TeradataStatementParser parser = new TeradataStatementParser(mrg_sql);
		List<SQLStatement> statementList = parser.parseStatementList();
		
		SQLStatement statement = statementList.get(0);
		
		Assert.assertTrue(statement instanceof TeradataMergeStatement);
		
		System.out.println(statement);
	}
	
	private void exec_test_oracle(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String mrg_sql = input.trim();
		
		OracleStatementParser parser = new OracleStatementParser(mrg_sql);
		List<SQLStatement> statementList = parser.parseStatementList();
		
		SQLStatement statement = statementList.get(0);
		
		Assert.assertTrue(statement instanceof OracleMergeStatement);
		
		System.out.println(statement);
	}
}
