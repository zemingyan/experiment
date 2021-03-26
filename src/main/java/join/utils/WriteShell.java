package main.java.join.utils;



import main.java.join.datastuct.Const;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class WriteShell {

    public static void createNodeData(String fileName, Integer num, Double low, Double high) throws IOException {
        BufferedWriter bufferedWriter;
        bufferedWriter = new BufferedWriter(new FileWriter(fileName));
        bufferedWriter.write(num + "\n");

        Random random = new Random();
        DoubleStream doubleStream = random.doubles(num, low,high);

        DoubleStream localStream = random.doubles(num, low + Const.NODE_INCREMENT,
                high + Const.NODE_INCREMENT);
        double[] local = localStream.toArray();
        DecimalFormat df = new DecimalFormat("#.00");
        AtomicInteger cnt = new AtomicInteger(1);
        doubleStream.forEach(v -> {
            try {
                double localValue = local[new Integer(String.valueOf(cnt)) - 1];
                bufferedWriter.write(cnt + " " + 1 + " " + df.format(localValue) + " " + df.format(v));
                bufferedWriter.write("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            cnt.getAndIncrement();

        });


        bufferedWriter.close();
    }


    public static void main(String[] args) throws IOException {
      /* createNodeData("/home/zemingyan/IdeaProjects/fast-unflod/src/main/resource/data/createNode.txt",
                15, 1.0, 10.0);*/
        createEdgeData("/home/zemingyan/IdeaProjects/fast-unflod/src/main/resource/data/edgedata.txt",
                1,7);
    }

    public static void createEdgeData(String fileName, Integer low, Integer high) throws IOException {

        String encoding = "UTF-8";
        File file = new File(fileName);
        if (file.isFile() && file.exists()) { // 判断文件是否存在
            InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            lineTxt = bufferedReader.readLine();

            System.out.println(lineTxt);
            // 预处理部分
            String head[] = lineTxt.split(" ");
            System.out.println(head[0]);
            Random random = new Random();
            DoubleStream doubleStream = random.doubles(Integer.valueOf(head[0]), low, low + Const.EDGE_INCREMENT);
            DoubleStream doubleStream_high = random.doubles(Integer.valueOf(head[0]), high,
                    high + Const.EDGE_INCREMENT);
            double[] high_values = doubleStream_high.toArray();
            double[] edgeValues = doubleStream.toArray();
            int cnt = 0;

            BufferedWriter bufferedWriter;
            bufferedWriter = new BufferedWriter(new FileWriter("/home/zemingyan/IdeaProjects/fast-unflod/src/main/resource/data/createEdge.txt"));

           DecimalFormat df = new DecimalFormat("#.00");
            bufferedWriter.write(head[0] + "\n");
            while ((lineTxt = bufferedReader.readLine()) != null) {
                String cur[] = lineTxt.split(" ");
                int u = Integer.parseInt(cur[0]);
                int v = Integer.parseInt(cur[1]);
                int rand = new Random().nextInt();
                if ((rand & 1) == 1) {
                    bufferedWriter.write(u + " " + v + " " + df.format(edgeValues[cnt++]) + "\n");
                }else {
                    bufferedWriter.write(u + " " + v + " " + df.format(high_values[cnt++]) + "\n");
                }

                System.out.println(u + " " + v + " " + df.format(edgeValues[cnt -1]) );
            }
            bufferedWriter.close();
        }


       /* BufferedWriter bufferedWriter;
        bufferedWriter = new BufferedWriter(new FileWriter(fileName));
        bufferedWriter.write(edgeNum + "\n");



        Random random = new Random();
        IntStream intStream = random.ints(edgeNum, 3, 20);
        int[] edgeValues = intStream.toArray();
        intStream = random.ints(edgeNum, 1, nodeNum);
        int[] fromNode = intStream.toArray();
        intStream = random.ints(edgeNum, 1, nodeNum);
        int[] endNode = intStream.toArray();





        for (int i = 0; i < edgeNum; i ++){
            bufferedWriter.write(fromNode[i] + " " + endNode[i] + " " + edgeValues[i] + "\n");
            System.out.println(fromNode[i] + " " + endNode[i] + " " + edgeValues[i] );
        }
        bufferedWriter.close();*/
    }
}
