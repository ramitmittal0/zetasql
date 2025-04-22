package org.example;

import com.google.zetasql.parser.ASTNodes.ASTFromClause;
import com.google.zetasql.parser.ParseTreeVisitor;

public class MyVisitor extends ParseTreeVisitor {

    @Override
    public void visit(ASTFromClause node) {
        super.visit(node);
    }        
}
