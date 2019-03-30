package helpers;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.*;
import java.util.List;

import static iterators.TableIterator.TABLE_DIRECTORY;

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
            file = new File(TABLE_DIRECTORY + mergedFilename);
            file.createNewFile();
            fileOutputStream = new FileOutputStream(file);
            bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            objectOutputStream = new ObjectOutputStream(bufferedOutputStream);

            fileInputStream = new FileInputStream(file);
            bufferedInputStream = new BufferedInputStream(fileInputStream);


        } catch (Exception e) {
        }

    }

    public void writeDataDisk(List<PrimitiveValue[]> sortedData) throws Exception { // TODO: Does it append?

        for (PrimitiveValue[] data : sortedData) {
            objectOutputStream.writeObject(data);
        }
        objectOutputStream.writeObject(null);
        objectOutputStream.close();
    }

    public PrimitiveValue[] getNext() {
        PrimitiveValue[] primitiveValues = null;
        try {
            if (objectInputStream == null)
                objectInputStream = new ObjectInputStream(bufferedInputStream);
            primitiveValues = (PrimitiveValue[]) objectInputStream.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            //return null;
        }
        //objectInputStream.close();
        return primitiveValues;
    }

    public void reset() throws Exception {
        if(fileInputStream!=null)
            fileInputStream.close();
        if(bufferedInputStream!=null)
            bufferedInputStream.close();
        if(objectInputStream != null)
            objectInputStream.close();

        fileInputStream = new FileInputStream(file);
        bufferedInputStream = new BufferedInputStream(fileInputStream);
        objectInputStream = new ObjectInputStream(bufferedInputStream);
    }
}

