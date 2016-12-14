package com.alibaba.druid.sql.dialect.teradata.ast;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataASTVisitor;

public interface TeradataSQLObject extends SQLObject {

	void accept0(TeradataASTVisitor visitor);
}
