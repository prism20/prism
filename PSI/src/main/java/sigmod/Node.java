package sigmod;

public class Node {

    public Node parent;
    public Node[] children;

    public byte value;

    public Node(Node parent, Node[] children, byte value) {
            this.parent = parent;
            this.children = children;
            this.value = value;
    }

}

