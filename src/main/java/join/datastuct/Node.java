package main.java.join.datastuct;

import main.java.FUEdge;

import java.util.stream.DoubleStream;

public class Node {
    public int type;
    public int nodeId;
    public double localTime, remoteTime;
    public boolean with_offload;

    public Node(){}
    public Node(Integer nodeId, Integer type, Double localTime, Double remoteTime){
        this.type = type;
        this.nodeId = nodeId;
        this.localTime = localTime;
        this.remoteTime = remoteTime;
    }
    public Node(Integer nodeId, Integer type, Double localTime, Double remoteTime, Boolean with_offload){
        this.type = type;
        this.nodeId = nodeId;
        this.localTime = localTime;
        this.remoteTime = remoteTime;
        this.with_offload = with_offload;
    }
    public Object clone() {
        Node temp = null;
        try {
            temp = (Node) super.clone();   //浅复制
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return temp;
    }
}
