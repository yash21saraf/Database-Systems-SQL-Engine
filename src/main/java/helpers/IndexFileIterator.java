package helpers;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.*;
import java.util.List;

import static helpers.CommonLib.TABLE_DIRECTORY;

public class IndexFileIterator {

    private String mergedFilename;
    private String tableName;
    private File file;

    private BufferedWriter bufferedWriter = null;
    private LineNumberReader lineNumberReader = null;

    public IndexFileIterator(String tableName, String mergedFilename) {
        this.tableName = tableName;
        this.mergedFilename = mergedFilename;

        try {

//            file = new File("tempfolder/" + mergedFilename);
//            file.createNewFile();
            file = new File(TABLE_DIRECTORY + mergedFilename);
            file.createNewFile();

            bufferedWriter = new BufferedWriter(new FileWriter(file, true));

        } catch (Exception e) {
        }
    }

    public void writeDataDisk(List<String> sortedData) throws Exception {

        for (String data : sortedData) {
            bufferedWriter.write(data + "\n");
        }
        //bufferedWriter.write("\n");
        bufferedWriter.close();
        bufferedWriter = null;
    }

    public String getNext() {
        String line = null;
        try {
            if (lineNumberReader == null)
                lineNumberReader = new LineNumberReader(new FileReader(file));

            line = lineNumberReader.readLine();

            if (line == null) {
                lineNumberReader.close();
                lineNumberReader = null;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return line;
    }

    public void reset() throws Exception {

    }

    public void clearAll() throws IOException {
        if (lineNumberReader != null) {
            lineNumberReader.close();
            lineNumberReader = null;
        }
        if (bufferedWriter != null) {
            bufferedWriter.close();
            bufferedWriter = null;
        }
    }
}