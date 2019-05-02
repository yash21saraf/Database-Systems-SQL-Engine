package iterators;

import builders.IteratorBuilder;
import dubstep.Main;
import helpers.CommonLib;
import helpers.Schema;
import helpers.Tuple;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.*;
import java.util.ArrayList;

//import static helpers.CommonLib.*;

public class TableIterator implements RAIterator
{

   //region Variables

   private ColumnDefinition[] columnDefinitions;
   private String tableName;
   private String tableAlias;
   private BufferedReader br;
   private FileReader fileReader;
   private PrimitiveValue[] currentLine;
   private PrimitiveValue[] nextLine;

   private Schema[] schema ;
   private boolean hasNextChecked = false;
   private boolean hasNextValue = false;
   private int cnter = 0;
   private CommonLib commonLib = CommonLib.getInstance();

   ///////////////////////////////////////////////////////
//   private Schema[] fileSchema ;
   private ColumnDefinition[] fileColumnDefinitions ;
   private ArrayList<Integer> newColDefMapping ;

   private Tuple tupleClass ;
   //endregion

   //region Constructor

   public TableIterator(String tableName,String tableAlias,ColumnDefinition[] columnDefinitions) throws Exception
   {
      this.fileColumnDefinitions= columnDefinitions;
      this.tableName = tableName;
      this.tableAlias = tableAlias;

      createColDef();

      this.tupleClass = new Tuple(this.columnDefinitions, tableName, newColDefMapping);
      if(this.tableAlias == null)
         this.schema = createSchema(this.columnDefinitions, tableName);
      else{
         this.schema = createSchema(this.columnDefinitions,this.tableAlias);
         addOriginalSchema(this.columnDefinitions, tableName);
      }

      try {
         File file = new File(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension) ;

         fileReader = new FileReader(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension);
         br = new LineNumberReader(fileReader);


      } catch (FileNotFoundException e) {
         throw e;
      }
   }

   private void createColDef(){
      ArrayList<ColumnDefinition> newColDef = new ArrayList<ColumnDefinition>() ;
      ArrayList<Integer> newColDefMapping = new ArrayList<Integer>() ;
      for(int i = 0; i < this.fileColumnDefinitions.length; i++){
         for(Column col : Main.columnList){
            if(col.getColumnName().equals(this.fileColumnDefinitions[i].getColumnName()) &&
            col.getTable().getName().equals(this.tableName)){
               newColDefMapping.add(i) ;
               newColDef.add(this.fileColumnDefinitions[i]);
            }
         }
      }
      this.columnDefinitions = newColDef.toArray(new ColumnDefinition[0]);
      this.newColDefMapping = newColDefMapping ;
   }

   private void addOriginalSchema(ColumnDefinition[] columnDefinitions, String tableName) throws Exception
   {

      Schema[] convertedTuple = new Schema[columnDefinitions.length];

      for (int index = 0; index < columnDefinitions.length; index++) {
         Schema convertedValue = new Schema();
         convertedValue.setColumnDefinition(columnDefinitions[index]);
         convertedValue.setTableName(tableName);
         convertedTuple[index] = convertedValue;

      }
      IteratorBuilder.iteratorSchemas.put(tableName, convertedTuple) ;
   }

   private Schema[] createSchema(ColumnDefinition[] columnDefinitions, String tableName) throws Exception
   {

      if (columnDefinitions == null)
         return null;

      Schema[] convertedTuple = new Schema[columnDefinitions.length];

      for (int index = 0; index < columnDefinitions.length; index++) {
         Schema convertedValue = new Schema();
         convertedValue.setColumnDefinition(columnDefinitions[index]);
         convertedValue.setTableName(tableName);
         convertedTuple[index] = convertedValue;

      }
      IteratorBuilder.iteratorSchemas.put(tableName, convertedTuple);
      return convertedTuple;
   }


   //endregion

   //region Iterator methods

//   @Override
//   public boolean hasNext() throws Exception
//   {
//      //if((cnter >= 1299) )
//       //  System.out.println(cnter);
//
//      try {
//         if (!hasNextChecked) {
//            hasNextChecked = true;
//            if ((nextLine = tupleClass.covertTupleToPrimitiveValue(br.readLine())) != null) {
//               cnter++;
//               hasNextValue = true;
//               return true;
//            }
//            hasNextValue = false;
//            fileReader.close();
//            br.close();
//            return false;
//         } else {
//            return hasNextValue;
//         }
//      }
//      catch (IOException e) {
//         throw e;
//      } catch (Exception e) {
//         throw e;
//      }
//   }

   @Override
   public boolean hasNext() throws Exception
   {
      //if((cnter >= 1299) )
      //  System.out.println(cnter);

      try {
         if (!hasNextChecked) {
            hasNextChecked = true;
            if ((nextLine = tupleClass.covertTupleToPrimitiveValuePP(br.readLine())) != null) {
               cnter++;
               hasNextValue = true;
               return true;
            }
            hasNextValue = false;
            fileReader.close();
            br.close();
            return false;
         } else {
            return hasNextValue;
         }
      }
      catch (IOException e) {
         throw e;
      } catch (Exception e) {
         throw e;
      }
   }

   @Override
   public PrimitiveValue[] next()
   {
      currentLine = nextLine;
      nextLine = null;
      hasNextChecked = false;
      return currentLine;
   }

   @Override
   public void reset() throws Exception
   {
      nextLine = null;
      currentLine = null;
      cnter = 0;
      try {
         br.close();
         fileReader.close();
         fileReader = new FileReader(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension);
         br = new BufferedReader(fileReader);
         cnter++;
         nextLine = tupleClass.covertTupleToPrimitiveValuePP(br.readLine());
      } catch (FileNotFoundException e) {
         throw e;
      } catch (IOException e) {
         throw e;
      } catch (Exception e) {
         throw e;
      }
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
      return this.schema;
   }

   @Override
   public void setSchema(Schema[] schema) {
      this.schema = schema;
   }

   @Override
   public RAIterator optimize(RAIterator iterator)
   {
      return iterator;
   }

   public String getTableAlias(){
      return this.tableAlias;
   }

   public ColumnDefinition[] getColumnDefinitions(){
      return this.columnDefinitions ;
   }

   public String getTableName(){
      return  tableName ;
   }

   public ArrayList<Integer> getNewColDefMapping(){
      return this.newColDefMapping ;
   }

   //endregion

}
