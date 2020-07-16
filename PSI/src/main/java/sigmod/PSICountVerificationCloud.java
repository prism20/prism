package sigmod;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class PSICountVerificationCloud {

    private int numRows;
    private int numThreads;
    private int numRowsPerThread;
    private int numOwners;
    private int delta, eta, g, eta2;

    public PSICountVerificationCloud(int numOwners, int delta, int eta, int g, int eta2, int numRows, int numThreads) {
        this.numRows = numRows;
        this.numThreads = numThreads;
        this.numRowsPerThread = numRows/numThreads;
        this.eta = eta;
        this.delta = delta;
        this.g = g;
        this.eta2 = eta2;

        this.numOwners = numOwners;

    }

    private void processBlock(List<int[][]> numbers, List<int[]> values) {
        Random random = new Random();
        int a1 = random.nextInt(delta);

        for (int j=0; j<numbers.size(); j++) {

            int value = Arrays.stream(numbers.get(j)[0]).sum();
            value = value % delta;
            value = value - a1;
            value = value % delta;
            if (value < 0) value += delta;
            value = Utils.power(g, value, eta2);

            int value2 = Arrays.stream(numbers.get(j)[0]).sum();
            value2 = value2 % delta;
            value2 = Utils.power(g, value2, eta2);

            //values.add(new int[]{value, value2});
            Collections.shuffle(numbers);
	}

    }

    public void process() throws SQLException, IOException {

        Instant start = Instant.now();

        int count = 0;

        List[] numbers = new List[numThreads];

        for (int i=0; i< numThreads; i++) {
            numbers[i] = new ArrayList();
        }

        List<int[]> values = Collections.synchronizedList(new ArrayList<int[]>());
        List<Thread> workers = new ArrayList<>();
        int currentThread = 0;

        System.out.println(" Time Fetch(sec): "+ Duration.between(start, Instant.now()));

        List<int[][]> resultSet = Utils.readMultiColMultiFiles(numOwners, numRows, 2,Common.DATA_DIR+Common.VER_P);

        for (int i=0; i<numRows; i++) {
            numbers[currentThread].add(resultSet.get(i));
            count += 1;

            if (count % numRowsPerThread == 0 && currentThread<numThreads-1) {
                List<int[][]> numbersThread = numbers[currentThread];

                workers.add(new Thread(()->processBlock(numbersThread, values)));
                currentThread += 1;
            }
        }

        List<int[][]> numbersThread = numbers[currentThread];
        workers.add(new Thread(()->processBlock(numbersThread, values)));

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

        int sum = values.stream().mapToInt(i->i[0]).sum();
        int sum2 = values.stream().mapToInt(i->i[1]).sum();

        Duration totalTime = Duration.between(start, Instant.now());

        System.out.println("Total sum1: "+ sum + "Total sum2: "+ sum2 + " Time (sec): "+ totalTime);

    }

    public static void main(String[] args) throws SQLException, IOException {

        PSICountVerificationCloud psiCloud = new PSICountVerificationCloud(
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
