package sigmod;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Bucketization {


    private int fanout = 10;
    private int max_range = 1000000;
    private int height;
    private byte[] data;
    private int fillFactor;

    public Bucketization(int fan_out, int height, byte data[]) {
            this.fanout = fan_out;
            this.height = height;
            this.data = data;
    }

    public Bucketization(int fan_out, int height) {
        this.fanout = fan_out;
        this.height = height;
        this.data = generateData();
    }

    public Bucketization(int fan_out, int height, int fillFactor) {
        this.fanout = fan_out;
        this.height = height;
        this.data = generateData(fillFactor);
    }


    public Node buildTree() {

        int levelNodes = data.length;
        Node[] nodes = new Node[levelNodes];
        Node[] currentNodes;

        for (int i=0; i<levelNodes; i++) {
            nodes[i] = new Node(null, null, data[i]);
        }

        for (int h=height; h>1; h--) {
            levelNodes = levelNodes/fanout;
            currentNodes = new Node[levelNodes];

            for (int n=0; n<levelNodes; n++) {
                currentNodes[n] = new Node(null, new Node[fanout], (byte)0);
                int k = 0;
                byte value = 0;
                for (int j=n*fanout; j<n*fanout+fanout; j+=1) {
                    currentNodes[n].children[k] = nodes[j];
                    if (nodes[j].value == 1) {
                        value = 1;
                    }
                    nodes[j].parent = currentNodes[n];
                    k += 1;
                }
                currentNodes[n].value = value;
            }
            nodes = currentNodes;


        }

        return nodes[0];


    }

    public byte[] generateData() {

        int total = Utils.power(fanout, height-1, 1000000000);
        Random rand = new Random(); //instance of random class
        byte[] data = new byte[total];

        for (int i=0; i<total; i++) {
            data[i] = (byte)rand.nextInt(2);
        }
        return data;
    }

    public byte[] generateData(int fillFactor) {

        int total = Utils.power(fanout, height-1, 1000000000);
        Random rand = new Random(); //instance of random class
        byte[] data = new byte[total];

        for (int i=0; i<total; i++) {
            int number =  rand.nextInt(100000);
            if (number > fillFactor) data[i] = 0;
            else data[i] = 1;
        }
        return data;
    }


    private void prettyPrintBuckets(Node root){

        if(root == null)
            return;
        Queue<Node> q =new LinkedList<Node>();
        q.add(root);

        while(true) {

            int nodeCount = q.size();
            if (nodeCount == 0)
                break;

            while (nodeCount > 0) {
                Node node = q.peek();
                System.out.print(node.value + " ");
                q.remove();
                if (node.children != null) {
                    for (int i = 0; i < node.children.length; i++) {
                        q.add(node.children[i]);
                    }
                }
                nodeCount--;
            }
            System.out.println();
            System.out.print("");
        }

    }

    public byte[][] getRows(Node root) {

        if(root == null)
            return null;

        Queue<Node> q =new LinkedList<Node>();
        q.add(root);

        int level = 1;

        byte[][] rows = new byte[height][];
        while(true) {

            int nodeCount = q.size();
            if (nodeCount == 0)
                break;
            rows[level-1] = new byte[(int)Math.pow(fanout, level-1)];


            int col = 0;
            while (nodeCount > 0) {
                Node node = q.peek();
                rows[level-1][col] = node.value;

                q.remove();
                if (node.children != null) {
                    for (int i = 0; i < node.children.length; i++) {
                        q.add(node.children[i]);
                    }
                }
                nodeCount--;
                col += 1;
            }
            System.out.println(level);
            level += 1;
        }
        return rows;
    }


    public void getCost(Node root, int startRow) {

        byte [][] rows = getRows(root);
        List<Integer> groups = new ArrayList<>();
        int total;

        groups.add((int)Math.pow(fanout, startRow));

        for (int j=startRow; j<height-1; j++) {
            int count = 0;
            System.out.println(j);

            for (int col=0; col<rows[j].length; col+=1) {
                if (rows[j][col] == 0) {
                    if (count != 0) {
                        groups.add(count*fanout);
                    }
                    count = 0;
                } else {
                    count += 1;
                }
            }
            if (count != 0) {
                groups.add(count*fanout);
            }
        }

        System.out.println("No. of Groups " + groups.size() + " Total Count " + groups.stream().mapToInt(Integer::intValue).sum());
    }

    public static void main(String[] args) throws SQLException, IOException {

	
        Bucketization bucketization = new Bucketization(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        Node root = bucketization.buildTree();

//        bucketization.prettyPrintBuckets(root);
        bucketization.getCost(root, 1);
    }

}

