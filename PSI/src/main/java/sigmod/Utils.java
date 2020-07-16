package sigmod;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

    public static int power(int x, int y, int p)
    {
        // Initialize result
        int res = 1;

        // Update x if it is more
        // than or equal to p
        x = x % p;

        if (x == 0) return 0; // In case x is divisible by p;

        while (y > 0)
        {
            // If y is odd, multiply x
            // with result
            if((y & 1)==1)
                res = (res * x) % p;

            // y must be even now
            // y = y / 2
            y = y >> 1;
            x = (x * x) % p;
        }
        return res;
    }

    public static List<Integer> readSingle(int numRows, String prefix) throws IOException {

        BufferedReader stringReader;
        List<Integer> numbers = new ArrayList<>();

        String fileName = String.format("%s.txt", prefix);
        stringReader = new BufferedReader(new FileReader(fileName));

        String line;
        int count = 0;

        for (int i=0 ; i<numRows; i++) {
            line = stringReader.readLine();
            numbers.add(Integer.parseInt(line));

            count += 1;
            //if (count % 100000 == 0) System.out.println(count + "Rows Found");

        }

        stringReader.close();

        return numbers;
    }


    public static List<int[]> readMulti(int numFiles, int numRows, String prefix) throws IOException {

        BufferedReader[] stringReaders = new BufferedReader[numFiles];
        List<int[]> numbers = new ArrayList<>();

        for (int i=0 ; i<numFiles; i++) {
            String fileName = String.format("%s%s.txt", prefix, i);
            stringReaders[i] = new BufferedReader(new FileReader(fileName));
        }

        String line;
        int count = 0;

        for (int i=0 ; i<numRows; i++) {
            numbers.add(new int[numFiles]);
            for (int j=0; j<numFiles; j++) {
                line = stringReaders[j].readLine();
                numbers.get(i)[j] = Integer.parseInt(line);

                count += 1;
                //if (count % 100000 == 0) System.out.println(count + "Rows Found");

            }
        }

        for (int i=0 ; i<numFiles; i++) {
            stringReaders[i].close();
        }

        return numbers;
    }

    public static List<int[][]> readMultiColMultiFiles(int numFiles, int numRows, int numCol, String prefix) throws IOException {

        BufferedReader[] stringReaders = new BufferedReader[numFiles];
        List<int[][]> numbers = new ArrayList<>();

        for (int i=0 ; i<numFiles; i++) {
            String fileName = String.format("%s%s.txt", prefix, i);
            stringReaders[i] = new BufferedReader(new FileReader(fileName));
        }

        String line;
        int count = 0;

        for (int i=0 ; i<numRows; i++) {
            numbers.add(new int[numFiles][numCol]);
            for (int j=0; j<numFiles; j++) {
                line = stringReaders[j].readLine();
                String[] cols = line.split(",");

                for (int k=0; k<numCol; k++) {
                    numbers.get(i)[j][k] = Integer.parseInt(cols[k]);
                }
                count += 1;
                //if (count % 100000 == 0) System.out.println(count + "Rows Found");

            }
        }

        for (int i=0 ; i<numFiles; i++) {
            stringReaders[i].close();
        }

        return numbers;
    }

}
