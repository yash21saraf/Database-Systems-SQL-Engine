package builders;

import iterators.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

import java.util.ArrayList;
import java.util.List;


public class Optimizer {

    /*RAIterator current;
    RAIterator parent;*/

    public void Optimizer(RAIterator current, RAIterator parent) {
      /*  this.current = current;
        this.parent = parent;*/
    }


    public RAIterator optimize(RAIterator current, List<Expression> expressionList) {

        if (current instanceof MapIterator) {


        } else if (current instanceof FilterIterator) {

            if (expressionList.size() > 0) {
                Expression filterExpression = ((FilterIterator) current).getExpression();
                List<Expression> currentFilterExpression = addExpressionToList(filterExpression);

                for (Expression expression : expressionList) {
                    if (!currentFilterExpression.contains(expression)) {
                        Expression modifiedExpression = getModifiedExpression(filterExpression, expression);
                        current = new FilterIterator(current.getChild(), modifiedExpression);
                    }
                }
            }

            FilterIterator filterIterator = (FilterIterator) current;
            Expression filterExpression = filterIterator.getExpression();

            expressionList = addExpressionToList(filterExpression);

            return optimize(current.getChild(), expressionList);

        } else if (current instanceof JoinIterator) {

        } else if (current instanceof OrderByIterator) {

        } else if (current instanceof TableIterator) {

        } else if (current instanceof GroupByIterator) {

        } else {
            return current;
        }


        return null;
    }

    private Expression getModifiedExpression(Expression filterExpression, Expression newExpression) {
        Expression modifiedExpression = new AndExpression(filterExpression, newExpression);
        return modifiedExpression;
    }

    private List<Expression> addExpressionToList(Expression filterExpression) {

        List<Expression> expressionList = new ArrayList<Expression>();

        if (filterExpression instanceof AndExpression) {
            AndExpression a = (AndExpression) filterExpression;
            expressionList.addAll(
                    addExpressionToList(a.getLeftExpression())
            );
            expressionList.addAll(
                    addExpressionToList(a.getRightExpression())
            );
        } else {
            expressionList.add(filterExpression);
        }
        return expressionList;
    }

}
