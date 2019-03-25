package iterators;

import helpers.BasicEval;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.*;
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
    //endregion

    //region Constructor

    public HavingIterator(RAIterator rootIterator, List<SelectItem> selectItems, Expression having) {

        this.currentIterator = rootIterator;
        this.selectItems = selectItems;
        this.having = having;

        this.origHavingExp = having;
        indexOfHavingExpression = findIndex(origHavingExp, selectItems);
    }

    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception {

        return currentIterator.hasNext();

    }

    @Override
    public PrimitiveValueWrapper[] next() throws Exception {

        PrimitiveValueWrapper[] primitiveValueWrappers = currentIterator.next();

        if (evaluateHavingExpression(having, primitiveValueWrappers[indexOfHavingExpression].getPrimitiveValue().toRawString()))
            return primitiveValueWrappers;

        else {
            while (currentIterator.hasNext()) {
                primitiveValueWrappers = currentIterator.next();

                indexOfHavingExpression = findIndex(origHavingExp, selectItems);

                if (evaluateHavingExpression(having, primitiveValueWrappers[indexOfHavingExpression].getPrimitiveValue().toRawString()))
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
        SelectExpressionItem backupHaving = new SelectExpressionItem();
        selectItemHaving.setExpression(havingExpression);
        backupHaving.setExpression(havingExpression);
        Expression value = null;

        if ((binaryExpression = (BinaryExpression) castAs(selectItemHaving.getExpression(), Expression.class)) != null) {
            if ((func = (Function) castAs(binaryExpression.getLeftExpression(), Function.class)) != null) {
                binaryExpression.setLeftExpression(new DoubleValue(out));
                value = binaryExpression.getRightExpression();
            } else if ((func = (Function) castAs(binaryExpression.getRightExpression(), Function.class)) != null) {
                binaryExpression.setRightExpression(new DoubleValue(out));
                value = binaryExpression.getLeftExpression();
            }

            try {
                if (basicEval.eval.eval(binaryExpression).toBool()) {

                    Function newFunc = new Function();

                    String condition = getCondition(binaryExpression); // TODO: Brute Forced -.-

                    String val = (value).toString();
                    newFunc.setName(func.getName() + func.getParameters().toString() + " " + condition + " " + val);
                    //newFunc.setName(func.getName() + " " + binaryExpression.getStringExpression() +" " + func.getParameters().toString());
                    selectItemHaving.setExpression(newFunc);

                    having = selectItemHaving.getExpression();
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
