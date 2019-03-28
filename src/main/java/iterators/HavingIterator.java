package iterators;

import helpers.BasicEval;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.List;

import static helpers.CommonLib.castAs;


public class HavingIterator implements RAIterator {

    //region Variables

    private RAIterator currentIterator;
    private List<SelectItem> selectItems;
    private Expression having;
    private long currentIndex = 0;
    private Expression origHavingExp;
    private int indexOfHavingExpression;
    private Schema[] schema;

    Expression exp;


    //endregion

    //region Constructor

    public HavingIterator(RAIterator rootIterator, List<SelectItem> selectItems, Expression having) {

        this.currentIterator = rootIterator;
        this.selectItems = selectItems;
        this.having = having;

        this.origHavingExp = having;
        indexOfHavingExpression = findIndex(origHavingExp, selectItems);


        if(having instanceof GreaterThan) {
            exp = new GreaterThan();
        } else if(having instanceof GreaterThanEquals) {
            exp = new GreaterThanEquals();
        } else if(having instanceof MinorThan) {
            exp = new MinorThan();
        } else if(having instanceof MinorThanEquals) {
            exp = new MinorThanEquals();
        } else if(having instanceof EqualsTo) {
            exp = new EqualsTo();
        } else if(having instanceof NotEqualsTo) {
            exp = new NotEqualsTo();
        }
    }

    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception {

        return currentIterator.hasNext();

    }

    @Override
    public PrimitiveValue[] next() throws Exception {

        PrimitiveValue[] primitiveValueWrappers = currentIterator.next();
        if(primitiveValueWrappers == null)
            return null;

        if (evaluateHavingExpression(having, primitiveValueWrappers[indexOfHavingExpression].toRawString()))
            return primitiveValueWrappers;

        else {
            while (currentIterator.hasNext()) {
                primitiveValueWrappers = currentIterator.next();
                if(primitiveValueWrappers == null)
                    return null;


                indexOfHavingExpression = findIndex(origHavingExp, selectItems);

                if (evaluateHavingExpression(having, primitiveValueWrappers[indexOfHavingExpression].toRawString()))
                    return primitiveValueWrappers;
            }
        }
        return null;
    }

    private int findIndex(Expression having, List<SelectItem> selectItems) {

        Function func;
        BinaryExpression binaryExpression;

        SelectExpressionItem selectItemHaving = new SelectExpressionItem();
        selectItemHaving.setExpression(having);

        String aggExpression = null;

        if ((binaryExpression = (BinaryExpression) castAs(selectItemHaving.getExpression(), Expression.class)) != null) {
            if ((func = (Function) castAs(binaryExpression.getLeftExpression(), Function.class)) != null) {
                aggExpression = func.getName() + func.getParameters().toString();

            } else if ((func = (Function) castAs(binaryExpression.getRightExpression(), Function.class)) != null) {
                aggExpression = func.getName() + func.getParameters().toString();
            }
        }

        int index = 0;
        for (SelectItem selectItem : selectItems) {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) CommonLib.castAs(selectItem, SelectExpressionItem.class);
            if (selectExpressionItem.getExpression().toString().equals(aggExpression))
                return index;
            index++;
        }
        return -1;
    }

    @Override
    public void reset() throws Exception {
        currentIterator.reset();
    }

    @Override
    public RAIterator getChild() {
        return currentIterator;
    }

    @Override
    public void setChild(RAIterator child) {
        this.currentIterator = child;
    }

    @Override
    public Schema[] getSchema() {
        return this.schema;
    }

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema;
    }

    /* Converts 'having' expression to SelectExpressionItem and unpack the expressions inside.
     * Assuming that 'having' clause always have only one aggregate function, i.e. on either leftExpression
     * or on rightExpression. If so, set the respective leftExpression or rightExpression to the
     * Expression value of 'finalize' result. Ex: "sum(a+d) > 3000" is converted to "250.0 > 3000"
     */
    private boolean evaluateHavingExpression(Expression havingExpression, String out) {

        PrimitiveValue hv[] = new PrimitiveValue[1];
        hv[0] = new DoubleValue(out);

        BasicEval basicEval = new BasicEval(hv);

        Expression backup = havingExpression;
        Function func;
        BinaryExpression binaryExpression;

        SelectExpressionItem selectItemHaving = new SelectExpressionItem();
        selectItemHaving.setExpression(havingExpression);

        SelectExpressionItem backupHaving = new SelectExpressionItem();
        backupHaving.setExpression(havingExpression);
        Expression value = null;

//        final BinaryExpression bex = (BinaryExpression) havingExpression;
//
//        BinaryExpression newBex = new BinaryExpression(bex.getLeftExpression(), bex.getRightExpression()) {
//            @Override
//            public void accept(ExpressionVisitor expressionVisitor) {
//
//            }
//
//            @Override
//            public String getStringExpression() {
//                return bex.getStringExpression().toString();
//            }
//        };




        if ((binaryExpression = (BinaryExpression) castAs(selectItemHaving.getExpression(), Expression.class)) != null) {
            if ((func = (Function) castAs(binaryExpression.getLeftExpression(), Function.class)) != null) {
                //newBex.setLeftExpression(new DoubleValue(out));
                ((GreaterThan) exp).setLeftExpression(new DoubleValue(out));
                value = binaryExpression.getRightExpression();
                ((GreaterThan) exp).setRightExpression(value);
            } else if ((func = (Function) castAs(binaryExpression.getRightExpression(), Function.class)) != null) {
                //newBex.setRightExpression(new DoubleValue(out));
                ((GreaterThan) exp).setRightExpression(new DoubleValue(out));
                value = binaryExpression.getLeftExpression();
                ((GreaterThan) exp).setLeftExpression(value);
            }

            try {
                //SelectExpressionItem evalExp =  new SelectExpressionItem();
                //evalExp.setExpression(newBex);
                //Expression e = (Expression) evalExp.getExpression();



                if (basicEval.eval.eval(exp).toBool()) {

                    Function newFunc = new Function();

                    String condition = getCondition(binaryExpression); // TODO: Brute Forced -.-

                    String val = (value).toString();
                    newFunc.setName(func.getName() + func.getParameters().toString() + " " + condition + " " + val);
                    selectItemHaving.setExpression(newFunc);

                    //having = bex;
                    return true;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        having = backup;
        return false;
    }

    private String getCondition(BinaryExpression binaryExpression) {

        String cond[] = binaryExpression.getStringExpression().toString().split(" ");

        return cond[0];
    }

    //endregion
}