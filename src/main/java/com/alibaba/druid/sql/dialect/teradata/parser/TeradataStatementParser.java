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
package com.alibaba.druid.sql.dialect.teradata.parser;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNCharExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataCreateTableStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataDeleteStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataInsertStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataMergeStatement;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.JdbcConstants;

public class TeradataStatementParser extends SQLStatementParser {

    public TeradataStatementParser(String sql){
        super(new TeradataExprParser(sql));
    }

    public TeradataStatementParser(Lexer lexer){
        super(new TeradataExprParser(lexer));
    }
    
    public TeradataExprParser getExprParser() {
    	return (TeradataExprParser) exprParser;
    }
    
    public TeradataSelectParser createSQLSelectParser() {
    	return new TeradataSelectParser(this.exprParser);
    }
    
    public SQLSelectStatement parseSelect() {
    	TeradataSelectParser selectParser = new TeradataSelectParser(this.exprParser);
    	return new SQLSelectStatement(selectParser.select(), JdbcConstants.TERADATA);
    }
    
    public SQLInsertStatement parseInsert() {
    	TeradataInsertStatement insertStatement = new TeradataInsertStatement();
    	
    	if (lexer.token() == Token.INSERT) {
    		lexer.nextToken();
    		
    		if (lexer.token() == Token.INTO) {
    			lexer.nextToken();
    		}
    		
    		SQLName tableName = this.exprParser.name();
    		insertStatement.setTableName(tableName);
    		
    		if (lexer.token() == Token.IDENTIFIER && !identifierEquals("VALUE")) {
    			insertStatement.setAlias(lexer.stringVal());
    			lexer.nextToken();
    		}
    	}
    	
    	int columnSize = 0;
        if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();
            if (lexer.token() == (Token.SELECT)) {
                SQLSelect select = this.exprParser.createSelectParser().select();
                select.setParent(insertStatement);
                insertStatement.setQuery(select);
            } else {
                this.exprParser.exprList(insertStatement.getColumns(), insertStatement);
                columnSize = insertStatement.getColumns().size();
            }
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.VALUES || identifierEquals("VALUE")) {
            lexer.nextTokenLParen();
            parseValueClause(insertStatement.getValuesList(), columnSize);
        } else if (lexer.token() == Token.SET) {
            lexer.nextToken();

            SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
            insertStatement.getValuesList().add(values);

            for (;;) {
                SQLName name = this.exprParser.name();
                insertStatement.addColumn(name);
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                } else {
                    accept(Token.COLONEQ);
                }
                values.addValue(this.exprParser.expr());

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }

        } else if (lexer.token() == (Token.SELECT) 
        		|| lexer.token() == (Token.SEL)) {
            SQLSelect select = this.exprParser.createSelectParser().select();
            select.setParent(insertStatement);
            insertStatement.setQuery(select);
        } else if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();
            SQLSelect select = this.exprParser.createSelectParser().select();
            select.setParent(insertStatement);
            insertStatement.setQuery(select);
            accept(Token.RPAREN);
        }

        return insertStatement;
    }
    
    private void parseValueClause(List<ValuesClause> valueClauseList, int columnSize) {
        for (;;) {
            if (lexer.token() != Token.LPAREN) {
                throw new ParserException("syntax error, expect ')'");
            }
            lexer.nextTokenValue();

            if (lexer.token() != Token.RPAREN) {
                List<SQLExpr> valueExprList;
                if (columnSize > 0) {
                    valueExprList = new ArrayList<SQLExpr>(columnSize);
                } else {
                    valueExprList = new ArrayList<SQLExpr>();
                }

                for (;;) {
                    SQLExpr expr;
                    if (lexer.token() == Token.LITERAL_INT) {
                        expr = new SQLIntegerExpr(lexer.integerValue());
                        lexer.nextTokenComma();
                    } else if (lexer.token() == Token.LITERAL_CHARS) {
                        expr = new SQLCharExpr(lexer.stringVal());
                        lexer.nextTokenComma();
                    } else if (lexer.token() == Token.LITERAL_NCHARS) {
                        expr = new SQLNCharExpr(lexer.stringVal());
                        lexer.nextTokenComma();
                    } else {
                        expr = exprParser.expr();
                    }

                    if (lexer.token() == Token.COMMA) {
                        valueExprList.add(expr);
                        lexer.nextTokenValue();
                        continue;
                    } else if (lexer.token() == Token.RPAREN) {
                        valueExprList.add(expr);
                        break;
                    } else {
                        expr = this.exprParser.primaryRest(expr);
                        if (lexer.token() != Token.COMMA && lexer.token() != Token.RPAREN) {
                            expr = this.exprParser.exprRest(expr);
                        }

                        valueExprList.add(expr);
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        } else {
                            break;
                        }
                    }
                }

                SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause(valueExprList);
                valueClauseList.add(values);
            } else {
                SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause(new ArrayList<SQLExpr>(0));
                valueClauseList.add(values);
            }

            if (lexer.token() != Token.RPAREN) {
                throw new ParserException("syntax error");
            }

            if (!parseCompleteValues && valueClauseList.size() >= parseValuesSize) {
                lexer.skipToEOF();
                break;
            }

            lexer.nextTokenComma();
            if (lexer.token() == Token.COMMA) {
                lexer.nextTokenLParen();
                continue;
            } else {
                break;
            }
        }
    }
    
    public SQLStatement parseCreate() {
        accept(Token.CREATE);
        
        if (lexer.token() == Token.TABLE 
        		|| lexer.token() == Token.VOLATILE
        		|| lexer.token() == Token.SET
        		|| lexer.token() == Token.MULTISET) {
        	TeradataCreateTableParser parser = new TeradataCreateTableParser(this.exprParser);
        	TeradataCreateTableStatement stmt = parser.parseCreateTable(false);
        	return stmt;
        }
    	
        throw new ParserException("TODO " + lexer.token());
    }
    
    public TeradataMergeStatement parseMerge() {
		accept(Token.MERGE);
    	
		TeradataMergeStatement stmt = new TeradataMergeStatement();
		
		accept(Token.INTO);
    	
		if (lexer.token() == Token.LPAREN) {
			lexer.nextToken();
			SQLSelect select = this.createSQLSelectParser().select();
			SQLSubqueryTableSource tableSource = new SQLSubqueryTableSource(select);
			stmt.setInto(tableSource);
			accept(Token.RPAREN);
		} else {
			stmt.setInto(exprParser.name());
		}
		
		stmt.setAlias(as());
		
		accept(Token.USING);
    	
		SQLTableSource using = this.createSQLSelectParser().parseTableSource();
		stmt.setUsing(using);
		
		accept(Token.ON);
		stmt.setOn(exprParser.expr());
		
		boolean insertFlag = false;
		if (lexer.token() == Token.WHEN) {
			lexer.nextToken();
			if (lexer.token() == Token.MATCHED) {
				TeradataMergeStatement.MergeUpdateClause updateClause = new TeradataMergeStatement.MergeUpdateClause();
				lexer.nextToken();
				accept(Token.THEN);
				accept(Token.UPDATE);
				accept(Token.SET);
				
				for (;;) {
					SQLUpdateSetItem item = this.exprParser.parseUpdateSetItem();
					
					updateClause.addItem(item);
					item.setParent(updateClause);
					
					if (lexer.token() == Token.COMMA) {
						lexer.nextToken();
						continue;
					}
					
					break;
				}
				
				if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    updateClause.setWhere(exprParser.expr());
                }

                if (lexer.token() == Token.DELETE) {
                    lexer.nextToken();
                    accept(Token.WHERE);
                    updateClause.setWhere(exprParser.expr());
                }

                stmt.setUpdateClause(updateClause);
			} else if (lexer.token() == Token.NOT) {
				lexer.nextToken();
				insertFlag = true;
			}
		}
		
		if (!insertFlag) {
			if (lexer.token() == Token.WHEN) {
				lexer.nextToken();
			}
			
			if (lexer.token() == Token.NOT) {
				lexer.nextToken();
				insertFlag = true;
			}
		}
		
		if (insertFlag) {
			TeradataMergeStatement.MergeInsertClause insertClause = new TeradataMergeStatement.MergeInsertClause();
			
			accept(Token.MATCHED);
			accept(Token.THEN);
			accept(Token.INSERT);
			
			if (lexer.token() == Token.LPAREN) {
				accept(Token.LPAREN);
				exprParser.exprList(insertClause.getColumns(), insertClause);
                accept(Token.RPAREN);
			}
			
			accept(Token.VALUES);
            accept(Token.LPAREN);
            exprParser.exprList(insertClause.getValues(), insertClause);
            accept(Token.RPAREN);

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                insertClause.setWhere(exprParser.expr());
            }

            stmt.setInsertClause(insertClause);			
		}
		
    	return stmt;
    }
    
    
    public TeradataDeleteStatement parseDeleteStatement() {
    	TeradataDeleteStatement deleteStatement = new TeradataDeleteStatement();

        if (lexer.token() == Token.DELETE
        		|| lexer.token() == Token.DEL) {
            lexer.nextToken();

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.IDENTIFIER) {
                deleteStatement.setTableSource(createSQLSelectParser().parseTableSource());

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();
                    SQLTableSource tableSource = createSQLSelectParser().parseTableSource();
                    deleteStatement.setFrom(tableSource);
                }
            } else if (lexer.token() == Token.FROM) {
                lexer.nextToken();
                deleteStatement.setTableSource(createSQLSelectParser().parseTableSource());
            } else {
                throw new ParserException("syntax error");
            }

            if (identifierEquals("USING")) {
                lexer.nextToken();

                SQLTableSource tableSource = createSQLSelectParser().parseTableSource();
                deleteStatement.setUsing(tableSource);
            }
        }

        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            SQLExpr where = this.exprParser.expr();
            deleteStatement.setWhere(where);
        }

        if (lexer.token() == (Token.ORDER)) {
            SQLOrderBy orderBy = exprParser.parseOrderBy();
            deleteStatement.setOrderBy(orderBy);
        }
        
        if (lexer.token() == Token.ALL) {
        	lexer.nextToken();
        	deleteStatement.setAll(new SQLIdentifierExpr(lexer.stringVal()));
        }

        return deleteStatement;
    }
    
    public SQLUpdateStatement parseUpdateStatement() {
    	return new TeradataUpdateParser(this.lexer).parseUpdateStatement();
    }
    
    public void parseStatementList(List<SQLStatement> statementList, int max) {
    	for (;;) {
            if (max != -1) {
                if (statementList.size() >= max) {
                    return;
                }
            }

            if (lexer.token() == Token.EOF) {
                return;
            }
            if (lexer.token() == Token.END) {
                return;
            }
            if (lexer.token() == Token.ELSE) {
                return;
            }

            if (lexer.token() == (Token.SEMI)) {
                lexer.nextToken();
                continue;
            }

            if (lexer.token() == (Token.SELECT)) {
                statementList.add(parseSelect());
                continue;
                // add merge for td.
            } else if (lexer.token() == Token.MERGE) {
                statementList.add(parseMerge());
                continue;
            } else {
                super.parseStatementList(statementList, max);	
            }
    	}
    }
    
    public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        if (lexer.token() == Token.SEL) {
        	statementList.add(parseSelect());
            return true;
        } else if (lexer.token() == Token.DEL) {
        	statementList.add(parseDeleteStatement());
        	return true;
        }
        return false;
    }

}
