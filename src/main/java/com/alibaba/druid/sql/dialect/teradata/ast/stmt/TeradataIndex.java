package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLIndex;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.teradata.ast.TeradataSQLObjectImpl;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;

public class TeradataIndex extends TeradataSQLObjectImpl implements SQLIndex, SQLTableElement {

    private SQLName                name;
    private List<SQLExpr>          columns = new ArrayList<SQLExpr>();
		
    private boolean                isUnique;
    private boolean                isPrimary;
    private boolean                isUniqueAndPrimary;
    
	@Override
	public List<SQLExpr> getColumns() {
		return columns;
	}

	@Override
	public SQLName getName() {
		return name;
	}

	@Override
	public void setName(SQLName name) {
		this.name = name;
	}

	@Override
	public void accept0(TeradataASTVisitor visitor) {
		if (visitor.visit(this)) {
            acceptChild(visitor, name);
            acceptChild(visitor, columns);
        }
        visitor.endVisit(this);
	}

	public boolean isUniqueIndex() {
		return isUnique;
	}
	
	public void setUniqueIndex(boolean isUnique) {
		this.isUnique = isUnique;
	}
	
	public boolean isPrimaryIndex() {
		return isPrimary;
	}
	
	public void setPrimaryIndex(boolean isPrimary) {
		this.isPrimary = isPrimary;
	}
	
	public boolean isUniqueAndPrimaryIndex() {
		return isUniqueAndPrimary;
	}
	
	public void setUniqueAndPrimaryIndex(boolean isUniqueAndPrimary) {
		this.isUniqueAndPrimary = isUniqueAndPrimary;
	}

}
