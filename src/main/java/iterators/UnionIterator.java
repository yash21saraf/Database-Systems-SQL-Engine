package iterators;

import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.statement.select.Union;

public class UnionIterator implements RAIterator
{

   //region Variables

   private Union union;
   private RAIterator[] plainSelectIterators;
   private RAIterator currentIterator;
   private int currentIndex = 0;

   //endregion

   //region Constructor

   public UnionIterator(Union union,RAIterator[] plainSelectIterators)
   {

      this.union = union;
      this.plainSelectIterators = plainSelectIterators;
      this.currentIterator = this.plainSelectIterators[currentIndex];

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
   public PrimitiveValueWrapper[] next() throws Exception
   {

      return currentIterator.next();

   }

   @Override
   public void reset() throws Exception
   {

      currentIterator.reset();

   }

   //endregion
}
