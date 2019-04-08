package iterators;

import helpers.Schema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.Union;

public class UnionIterator implements RAIterator
{

   //region Variables

   private Union union;
   private RAIterator[] plainSelectIterators;
   private RAIterator currentIterator;
   private int currentIndex = 0;
   private Schema[] schema ;

   //endregion

   //region Constructor

   public UnionIterator(Union union,RAIterator[] plainSelectIterators)
   {

      this.union = union;
      this.plainSelectIterators = plainSelectIterators;
      this.currentIterator = this.plainSelectIterators[currentIndex];
      this.schema = currentIterator.getSchema();

   }

   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {

      if (!currentIterator.hasNext()) {
         currentIndex++;
         if (currentIndex < plainSelectIterators.length)
            currentIterator = plainSelectIterators[currentIndex];
         else
            return false;
      }
      return true;

   }

   @Override
   public PrimitiveValue[] next() throws Exception
   {

      return currentIterator.next();

   }

   @Override
   public void reset() throws Exception
   {

      currentIterator.reset();

   }

   @Override
   public RAIterator getChild() {
      return null;
   }

   @Override
   public void setChild(RAIterator child) {
   }

   @Override
   public Schema[] getSchema() {
      return this.schema ;
   }

   @Override
   public void setSchema(Schema[] schema) {
      this.schema = schema ;
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
