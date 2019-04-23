package helpers;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.*;
import java.util.List;

import static helpers.CommonLib.TABLE_DIRECTORY;

public class FileIterator {

    private String mergedFilename;
    private String tableName;
    private File file;

    private FileOutputStream fileOutputStream;
    private BufferedOutputStream bufferedOutputStream;
    private ObjectOutputStream objectOutputStream;

    private BufferedInputStream bufferedInputStream;
    private FileInputStream fileInputStream;
    private ObjectInputStream objectInputStream;

    public FileIterator(String tableName, String mergedFilename) {
        this.tableName = tableName;
        this.mergedFilename = mergedFilename;

        try {

//            file = new File("tempfolder/" + mergedFilename);
//            file.createNewFile();
            file = new File(TABLE_DIRECTORY + mergedFilename);
            file.createNewFile();
            fileOutputStream = new FileOutputStream(file);
            bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            objectOutputStream = new ObjectOutputStream(bufferedOutputStream);

        } catch (Exception e) {
        }
    }

    public void writeDataDisk(List<PrimitiveValue[]> sortedData) throws Exception {

        for (PrimitiveValue[] data : sortedData) {
            objectOutputStream.writeUnshared(data);
        }
//        objectOutputStream.reset();
        objectOutputStream.writeUnshared(null);
        objectOutputStream.close();
        bufferedOutputStream.close();
        fileOutputStream.close() ;

        objectOutputStream = null ;
        bufferedOutputStream = null ;
        fileOutputStream= null ;

        fileInputStream = new FileInputStream(file);
        bufferedInputStream = new BufferedInputStream(fileInputStream);
    }

    public PrimitiveValue[] getNext() {
        PrimitiveValue[] primitiveValues = null;
        try {
            if(fileInputStream == null)
                fileInputStream = new FileInputStream(file);
            if(bufferedInputStream == null)
                bufferedInputStream = new BufferedInputStream(fileInputStream);
            if (objectInputStream == null)
                objectInputStream = new ObjectInputStream(bufferedInputStream);

            primitiveValues = (PrimitiveValue[]) objectInputStream.readUnshared();

            if (primitiveValues == null) {
                objectInputStream.close();
                bufferedInputStream.close();
                fileInputStream.close() ;
                objectInputStream = null ;
                bufferedInputStream = null;
                fileInputStream = null ;
            }
        } catch (Exception e) {
            e.printStackTrace();
            //return null;
        }
        return primitiveValues;
    }

    public void reset() throws Exception {
        if (fileInputStream != null)
            fileInputStream.close();
        if (bufferedInputStream != null)
            bufferedInputStream.close();
        if (objectInputStream != null)
            objectInputStream.close();

        fileInputStream = new FileInputStream(file);
        bufferedInputStream = new BufferedInputStream(fileInputStream);
        objectInputStream = new ObjectInputStream(bufferedInputStream);
    }

    public void clearAll() throws IOException {
        if (fileInputStream != null)
            fileInputStream.close();
        if (bufferedInputStream != null)
            bufferedInputStream.close();
        if (objectInputStream != null)
            objectInputStream.close();
        if (objectOutputStream != null)
            objectOutputStream.close();
        if (fileOutputStream != null)
            fileOutputStream.close();
        if (bufferedOutputStream!= null)
            bufferedOutputStream.close();

        fileInputStream = null ;
        bufferedInputStream= null;
        objectInputStream = null;
        objectOutputStream = null ;
        fileOutputStream= null ;
        bufferedOutputStream= null ;
    }
}