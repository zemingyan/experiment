package main.java.join.datastuct;

import main.java.FUEdge;

import java.util.stream.DoubleStream;

public class Edge {
    public int to, next;
    public double weight; //暂定为传输时间   最终要修改为  数据量/带宽

    public Edge(){}
   /* public Edge(Integer to, Double weight){
        this.to = to;
        this.weight = weight;
    }*/

    public Object clone() {
        Edge temp = null;
        try {
            temp = (Edge) super.clone();   //浅复制
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return temp;
    }
}
