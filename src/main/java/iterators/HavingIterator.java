package iterators;

import helpers.BasicEval;
import helpers.CommonLib;
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
    SelectExpressionItem selectItemHaving = new SelectExpressionItem();


    //endregion

    //region Constructor

    public HavingIterator(RAIterator rootIterator, List<SelectItem> selectItems, Expression having) {

        this.currentIterator = rootIterator;
        this.selectItems = selectItems;
        this.having = having;
        this.schema = rootIterator.getSchema();

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
        Function func;
        BinaryExpression binaryExpression;

        selectItemHaving.setExpression(havingExpression);

        Expression value = null;

        if ((binaryExpression = (BinaryExpression) castAs(selectItemHaving.getExpression(), Expression.class)) != null) {
            if ((func = (Function) castAs(binaryExpression.getLeftExpression(), Function.class)) != null) {

                if(having instanceof GreaterThan) {
                    ((GreaterThan) exp).setLeftExpression(new DoubleValue(out));
                    value = binaryExpression.getRightExpression();
                    ((GreaterThan) exp).setRightExpression(value);
                } else if(having instanceof GreaterThanEquals) {
                    ((GreaterThanEquals) exp).setLeftExpression(new DoubleValue(out));
                    value = binaryExpression.getRightExpression();
                    ((GreaterThanEquals) exp).setRightExpression(value);
                } else if(having instanceof MinorThan) {
                    ((MinorThan) exp).setLeftExpression(new DoubleValue(out));
                    value = binaryExpression.getRightExpression();
                    ((MinorThan) exp).setRightExpression(value);
                } else if(having instanceof MinorThanEquals) {
                    ((MinorThanEquals) exp).setLeftExpression(new DoubleValue(out));
                    value = binaryExpression.getRightExpression();
                    ((MinorThanEquals) exp).setRightExpression(value);
                } else if(having instanceof EqualsTo) {
                    ((EqualsTo) exp).setLeftExpression(new DoubleValue(out));
                    value = binaryExpression.getRightExpression();
                    ((EqualsTo) exp).setRightExpression(value);
                } else if(having instanceof NotEqualsTo) {
                    ((NotEqualsTo) exp).setLeftExpression(new DoubleValue(out));
                    value = binaryExpression.getRightExpression();
                    ((NotEqualsTo) exp).setRightExpression(value);
                }

            } else if ((func = (Function) castAs(binaryExpression.getRightExpression(), Function.class)) != null) {

                if(having instanceof GreaterThan) {
                    ((GreaterThan) exp).setRightExpression(new DoubleValue(out));
                    value = binaryExpression.getLeftExpression();
                    ((GreaterThan) exp).setLeftExpression(value);
                } else if(having instanceof GreaterThanEquals) {
                    ((GreaterThanEquals) exp).setRightExpression(new DoubleValue(out));
                    value = binaryExpression.getLeftExpression();
                    ((GreaterThanEquals) exp).setLeftExpression(value);
                } else if(having instanceof MinorThan) {
                    ((MinorThan) exp).setRightExpression(new DoubleValue(out));
                    value = binaryExpression.getLeftExpression();
                    ((MinorThan) exp).setLeftExpression(value);
                } else if(having instanceof MinorThanEquals) {
                    ((MinorThanEquals) exp).setRightExpression(new DoubleValue(out));
                    value = binaryExpression.getLeftExpression();
                    ((MinorThanEquals) exp).setLeftExpression(value);
                } else if(having instanceof EqualsTo) {
                    ((EqualsTo) exp).setRightExpression(new DoubleValue(out));
                    value = binaryExpression.getLeftExpression();
                    ((EqualsTo) exp).setLeftExpression(value);
                } else if(having instanceof NotEqualsTo) {
                    ((NotEqualsTo) exp).setRightExpression(new DoubleValue(out));
                    value = binaryExpression.getLeftExpression();
                    ((NotEqualsTo) exp).setLeftExpression(value);
                }
            }

            try {
                if (basicEval.eval.eval(exp).toBool()) {
                    return true;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private String getCondition(BinaryExpression binaryExpression) {

        String cond[] = binaryExpression.getStringExpression().toString().split(" ");

        return cond[0];
    }

    @Override
    public RAIterator optimize(RAIterator iterator)
    {
        RAIterator child = iterator.getChild();
        child = child.optimize(child);
        iterator.setChild(child);
        return iterator;
    }

    //endregion
}