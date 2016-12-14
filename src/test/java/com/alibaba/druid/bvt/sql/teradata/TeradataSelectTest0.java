package com.alibaba.druid.bvt.sql.teradata;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import junit.framework.TestCase;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelectQueryBlock;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.Utils;

public class TeradataSelectTest0 extends TestCase{
	public void test_0() throws Exception {
//		exec_test("bvt/parser/teradata-1.txt");
//		exec_test("bvt/parser/teradata-2.txt");
//		exec_test("bvt/parser/teradata-3.txt");
//		exec_test("bvt/parser/teradata-sel-0.txt");
		exec_test("bvt/parser/teradata-sel-1.txt");
	}
	
	public void exec_test(String resource) throws Exception {
		System.out.println(resource);
		InputStream is = null;
		
		is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
		Reader reader = new InputStreamReader(is, "UTF-8");
		String input = Utils.read(reader);
		String sql = input.trim();
		
//		System.out.println(sql);
		
//		MySqlStatementParser parser = new MySqlStatementParser(sql);
		TeradataStatementParser parser = new TeradataStatementParser(sql);
        
		List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);
        
        if(stmt instanceof SQLSelectStatement) {
        	System.out.println("yes, is SELECT of td");
        } else {
        	System.out.println("nop...");
        }
        
        System.out.println("stmt: " + stmt);
		SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
		SQLSelect selectSQL = selectStmt.getSelect();
		
//		MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) selectSQL.getQuery();
		TeradataSelectQueryBlock queryBlock = (TeradataSelectQueryBlock) selectSQL.getQuery();
		System.out.println("select list: " + queryBlock.getSelectList());
        System.out.println("select item size: " + queryBlock.getSelectList().size());
        for (int i=0; i<queryBlock.getSelectList().size(); i++) {
        	SQLSelectItem selectItem = queryBlock.getSelectList().get(i);
            SQLExpr expr = selectItem.getExpr();
        	System.out.println("select item: " + expr.toString());
            if (expr instanceof SQLAggregateExpr) {
            	System.out.println("order by: " + ((SQLAggregateExpr) expr).getOver().getOrderBy().getItems().get(1).getExpr());
            	System.out.println("partition by: " + ((SQLAggregateExpr) expr).getOver().getPartitionBy().get(1));
            }
            if (expr instanceof SQLCaseExpr) {
            	System.out.println("when expr: " + ((SQLCaseExpr)expr).getItems().get(0).getConditionExpr());
            	System.out.println("when value expr: " + ((SQLCaseExpr)expr).getItems().get(0).getValueExpr());
            	System.out.println("else expr: " + ((SQLCaseExpr)expr).getElseExpr());
            	System.out.println("case as: " + ((SQLCaseExpr)expr).getParent());
            }
        }
        System.out.println("from: " + queryBlock.getFrom());
        
        SQLJoinTableSource joinSource = (SQLJoinTableSource) queryBlock.getFrom();
        System.out.println("join type: " + joinSource.getJoinType());
        if (joinSource.getLeft() instanceof SQLJoinTableSource) {
        	System.out.println("yes, of SQL join");
        	SQLJoinTableSource joinLeft = ((SQLJoinTableSource) joinSource.getLeft());
        	SQLJoinTableSource joinLeft_2 = ((SQLJoinTableSource) joinLeft.getLeft());
        	System.out.println("join type in left: " + joinLeft_2.getJoinType());
        	System.out.println("left in left: " + joinLeft_2.getLeft());
        	System.out.println("right in left: " + joinLeft_2.getRight());
        	System.out.println("join condition in left: " + joinLeft_2.getCondition());
        }
        System.out.println("join left: " + joinSource.getLeft());
        System.out.println("join right: " + joinSource.getRight());
        System.out.println("join condition: " + joinSource.getCondition());
        
        
//        OracleSchemaStatVisitor visitor = new OracleSchemaStatVisitor();
        TeradataSchemaStatVisitor visitor = new TeradataSchemaStatVisitor();
        selectSQL.accept(visitor);
        System.out.println("relationships: " + visitor.getRelationships());
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
	}

}
