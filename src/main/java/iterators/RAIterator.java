package iterators;

import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public interface RAIterator
{

   public boolean hasNext() throws Exception;

   public PrimitiveValue[] next() throws Exception;

   public void reset() throws Exception;

   public RAIterator getChild();

   public void setChild(RAIterator child);

   public ColumnDefinition[] getColumnDefinition();

   public void setColumnDefinition(ColumnDefinition[] columnDefinition);

   public void setTableName(String tableName);

   public String getTableName();

   public void setTableAlias(String tableAlias);

   public String getTableAlias() ;
}
