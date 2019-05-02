package helpers;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;

public class Tuple {

    private String[] dataTypes;
    private int rowSize;
    private String table;
    private ArrayList<Integer> newColDefMapping ;

    public Tuple(ColumnDefinition[] columnDefinitions, String table) {

        this.table = table;

        this.dataTypes = new String[columnDefinitions.length];

        for (int i = 0; i < columnDefinitions.length; i++) {
            dataTypes[i] = columnDefinitions[i].getColDataType().getDataType();
        }

        rowSize = columnDefinitions.length;
    }

    public Tuple(ColumnDefinition[] columnDefinitions, String table, ArrayList<Integer> newColDefMapping) {

        this.table = table;

        this.dataTypes = new String[columnDefinitions.length];

        for (int i = 0; i < columnDefinitions.length; i++) {
            dataTypes[i] = columnDefinitions[i].getColDataType().getDataType();
        }
        this.newColDefMapping = newColDefMapping ;

        rowSize = columnDefinitions.length;
    }

    public PrimitiveValue[] covertTupleToPrimitiveValue(String tupleString) throws Exception {

        if(tupleString == null)
            return null;

        String[] tupleArray = tupleString.split("\\|");

        if(table != null) {
            if (table.equals("LINEITEM")) {
                tupleArray[13] = null;
                tupleArray[15] = null;
            } else if (table.equals("ORDERS")) {
                tupleArray[6] = null;
                tupleArray[8] = null;
            } else if (table.equals("SUPPLIER")) {
                tupleArray[2] = null;
                tupleArray[6] = null;
            } else if (table.equals("PARTSUPP")) {
                tupleArray[4] = null;
            }
        }

        PrimitiveValue[] convertedTuple = new PrimitiveValue[tupleArray.length];

        for (int index = 0; index < rowSize; index++) {
            PrimitiveValue convertedValue;
            convertedValue = convertToPrimitiveValue(tupleArray[index], dataTypes[index]);
            if (convertedValue != null) {
                convertedTuple[index] = convertedValue;
            } else {
                throw new Exception("Invalid columnType.");
            }
        }
        return convertedTuple;

    }

    public PrimitiveValue[] covertTupleToPrimitiveValuePP(String tupleString) throws Exception {

        if(tupleString == null)
            return null;

        String[] tupleArray = tupleString.split("\\|");

        PrimitiveValue[] convertedTuple = new PrimitiveValue[rowSize];

        for (int index = 0; index < rowSize; index++) {
            PrimitiveValue convertedValue = convertToPrimitiveValue(tupleArray[newColDefMapping.get(index)], dataTypes[index]);
            if (convertedValue != null) {
                convertedTuple[index] = convertedValue;
            } else {
                throw new Exception("Invalid columnType.");
            }
        }
        return convertedTuple;

    }

    public PrimitiveValue convertToPrimitiveValue(String value, String dataType) {

        switch (types.valueOf(dataType)) {

            case STRING:
                return new StringValue(value);

            case INT:
                return new LongValue(value);

            case DECIMAL:
                return new DoubleValue(value);

            case DATE:
                return new DateValue(value);

            default:
                return new StringValue(value);
        }
    }

    private enum types {STRING, CHAR, VARCHAR, DECIMAL, INT, DATE}

}