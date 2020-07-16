package sigmod;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class PSISumVerificationCloud {

    private int numRows;
    private int numThreads;
    private int numRowsPerThread;
    private int numOwners;
    private int delta, eta, g, eta2;

    public PSISumVerificationCloud(int numOwners, int delta, int eta, int g, int eta2, int numRows, int numThreads) {
        this.numRows = numRows;
        this.numThreads = numThreads;
        this.numRowsPerThread = numRows/numThreads;
        this.eta = eta;
        this.delta = delta;
        this.g = g;
        this.eta2 = eta2;

        this.numOwners = numOwners;

    }

    private void processBlock(List<int[]> numbers, List<Integer> multiply, List<Integer> values) {
        Random random = new Random();
        int a1 = random.nextInt(delta);

        for (int j=0; j<numbers.size(); j++) {

            int value = Arrays.stream(numbers.get(j)).sum();
            value = value % delta;
            value = value - a1;
            value = (value*multiply.get(j)) % delta;
            if (value < 0) value += delta;
            value = Utils.power(g, value, eta2);
            value = (value*multiply.get(j)) % delta;

       //     values.add(value);
        }

    }

    public void process() throws SQLException, IOException {

        Instant start = Instant.now();

        int count = 0;

        List[] numbers = new List[numThreads];
        List[] multiply = new List[numThreads];

        for (int i=0; i< numThreads; i++) {
            numbers[i] = new ArrayList();
            multiply[i] = new ArrayList();
        }

        List<Integer> values = Collections.synchronizedList(new ArrayList<Integer>());
        List<Thread> workers = new ArrayList<>();
        int currentThread = 0;

        System.out.println(" Time Fetch(sec): "+ Duration.between(start, Instant.now()));

        List<int[]> resultSet = Utils.readMulti(numOwners, numRows, Common.DATA_DIR+Common.PSI_P);
        List<Integer> multiplySet = Utils.readSingle(numRows, Common.DATA_DIR+Common.SUM_P);
        List<Integer> multiplySet2 = Utils.readSingle(numRows, Common.DATA_DIR+Common.SUM_P);

        for (int i=0; i<numRows; i++) {
            numbers[currentThread].add(resultSet.get(i));
            multiply[currentThread].add(multiplySet.get(i));
            count += 1;

            if (count % numRowsPerThread == 0 && currentThread<numThreads-1) {
                List<int[]> numbersThread = numbers[currentThread];
                List<Integer> multiplyThread = multiply[currentThread];

                workers.add(new Thread(()->processBlock(numbersThread, multiplyThread, values)));
                currentThread += 1;
            }
        }

        List<int[]> numbersThread = numbers[currentThread];
        List<Integer> multiplyThread = multiply[currentThread];

        workers.add(new Thread(()->processBlock(numbersThread, multiplyThread, values)));

        for (int i=0; i<numThreads; i++) {
            workers.get(i).start();
        }

        for (int i=0; i<numThreads; i++) {
            try {
                workers.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        int sum = values.stream().mapToInt(i->i).sum();
        Duration totalTime = Duration.between(start, Instant.now());

        System.out.println("Total count: "+ sum + " Time (sec): "+ totalTime);

    }

    public static void main(String[] args) throws SQLException, IOException {

        PSISumVerificationCloud psiCloud = new PSISumVerificationCloud(
		Integer.parseInt(args[0]),
                113,
                227,
                3,
                227*2,
                Integer.parseInt(args[1]),
                Integer.parseInt(args[2])
	
			);
        psiCloud.process();

    }


}
