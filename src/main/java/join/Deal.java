package main.java.join;

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import jdk.internal.org.objectweb.asm.util.CheckAnnotationAdapter;
import main.java.FUEdge;
import main.java.join.datastuct.Common;
import main.java.join.datastuct.Const;
import main.java.join.datastuct.Edge;
import main.java.join.datastuct.Node;
import org.junit.Test;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class Deal {

    Node[] nodes;
    Edge[] edges;

    int head[]; // 头结点下标
    int cluster[];
    double cluster_weight[];
    int top; // 已用E的个数
    int allNodeNum, length;
    int out_edge_num[]; //节点出度边数
    int in_edge_num[];
    double out_edge_total_value[];
    double in_edge_total_value[];



    void addEdge(int from, int to, double weight) {
        if (edges[top] == null)
            edges[top] = new Edge();

        edges[top].to = to;
        edges[top].weight = weight;
        edges[top].next = head[from];
        head[from] = top++;

        out_edge_total_value[from] += weight;
        in_edge_total_value[to] += weight;
        out_edge_num[from] ++; //节点from 出度边
        in_edge_num[to] ++;
    }



    void init() {

        String edgeInfoPath = "/home/zemingyan/IdeaProjects/fast-unflod/src/main/resource/data/testEdge.txt";
        String nodeInfoPath = "/home/zemingyan/IdeaProjects/fast-unflod/src/main/resource/data/testNode.txt";
        try {
            String encoding = "UTF-8";
            File file = new File(nodeInfoPath);
            if (file.isFile() && file.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(new FileInputStream(nodeInfoPath), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                lineTxt = bufferedReader.readLine();
                System.out.println(lineTxt);
                Integer nodeNum = Integer.valueOf(lineTxt);

                allNodeNum = nodeNum + 1;
                length = allNodeNum + 10;
                //cluster_weight = new double[allNodeNum];
                cluster = new int[length];
                out_edge_num = new int[length];
                in_edge_num = new int[length];
                out_edge_total_value = new double[length];
                in_edge_total_value = new double[length];
                head = new int[length];
                for (int i = 1; i < length; i ++){
                    head[i] = -1;
                    cluster[i] = i;
                    out_edge_num[i] = in_edge_num [i] = 0;
                }

                nodes = new Node[length];
                int cnt = 1;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String cur[] = lineTxt.split(" ");
                    Integer nodeId = Integer.valueOf(cur[0]);
                    Integer type = Integer.valueOf(cur[1]);
                    Double localTime = Double.valueOf(cur[2]);
                    Double remoteTime = Double.valueOf(cur[3]);
                    //cluster_weight
                    nodes[cnt++] = new Node(nodeId, type, localTime, remoteTime);
                }

                read.close();
            }
            File edgeFile = new File(edgeInfoPath);
            if (edgeFile.isFile() && edgeFile.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(new FileInputStream(edgeInfoPath), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                lineTxt = bufferedReader.readLine();
                System.out.println(lineTxt);
                Integer edgeNum = Integer.valueOf(lineTxt);

                edges = new Edge[edgeNum];
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String cur[] = lineTxt.split(" ");
                    Integer from = Integer.valueOf(cur[0]);
                    Integer to = Integer.valueOf(cur[1]);
                    Double weight = Double.valueOf(cur[2]);
                    addEdge(from, to, weight);
                }
                read.close();
            }



        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
        check_offload();
    }

    private void check_offload() {
        for (int i = 1; i < allNodeNum; i ++){
            if (cluster[i] == path_compress(i)){
                double time = nodes[i].localTime - nodes[i].remoteTime - in_edge_total_value[i] - out_edge_total_value[i];
                if (time > Const.OFFLOAD_VALUE){
                    nodes[i].with_offload = true;
                }else {
                    nodes[i].with_offload = false;
                }
            }
        }
    }

    void check(int node_id, String msg){
        for (int x = head[node_id]; x != -1; x = edges[x].next){
            System.out.println(msg + "  check++++节点" + node_id +"的临边 " + edges[x].to);
        }
    }
    void check_all(){
        for (int i = 1; i < allNodeNum; i ++){
           // if (edges[head[path_compress(i)]].next != -1)
                System.out.println("节点" + i + "的临接点是 %%%%%%%%%%%%%%%%%%%%%%%");

            int the_father = path_compress(i);
            for (int x = head[the_father]; x != -1; x = edges[x].next){
                System.out.print(" " + edges[x].to);
            }System.out.println();
        }
    }
    void check_all_after(){
        int temp[] = new int[allNodeNum];
        int len = 0;
        boolean[] vis = new boolean[allNodeNum];
        for (int i = 1; i < allNodeNum; i ++){
            int value = path_compress(i);
            if (!vis[value]){
                vis[value] = true;
                temp[len++] = value;
            }
        }
        for (int i = 0; i < len; i ++){
            System.out.println("check_after节点" + temp[i] + "的临接点是 %%%%%%%%%%%%%%%%%%%%%%%");

            int the_father = path_compress(temp[i]);
            for (int x = head[the_father]; x != -1; x = edges[x].next){
                System.out.print(" " + edges[x].to);
            }System.out.println();
        }
    }
    void join(){


       // check_all();

        serial_merge();
        check_all_after();
        parallel_merge();
        check_all_after();

        System.out.println("============族群情况==========");
        for (int i = 1; i < allNodeNum; i ++){
            int j = i - 1;
            if (j % 5 == 0) System.out.println("      ");
            path_compress(i);
            System.out.print(cluster[i] + " ");
        }System.out.println();

        System.out.println("============offload情况==========");
        for (int i = 1; i < allNodeNum; i ++){
            if (path_compress(i) == i)
            System.out.println(i + " " + nodes[i].with_offload);
        }

        double offload_cnt = 0D;
        for (int i = 1; i < allNodeNum; i ++){
            if (nodes[path_compress(i)].with_offload) offload_cnt++;
        }
        System.out.println("end join 前的情况===================================");
        if (offload_cnt/allNodeNum > Const.LOAD_FACTOR){  // 分散节点合并

            System.out.println("分散节点合并");
            end_merge();
        }
        System.out.println("============end join后offload情况==========");
        for (int i = 1; i < allNodeNum; i ++){
            if (path_compress(i) == i)
                System.out.println(i + " " + nodes[i].with_offload);
        }

        System.out.println("-----------一轮结束后边的变化-------------");
     /*   System.out.println(path_compress(11));
        for (int i = 1; i < allNodeNum; i ++){
            int cluster = path_compress(i);
            check(cluster);
        }*/

        System.out.println("===========end join后=族群情况==========");
        for (int i = 1; i < allNodeNum; i ++){
            int j = i - 1;
            if (j % 5 == 0) System.out.println("      ");
            path_compress(i);
            System.out.print(cluster[i] + " ");
        }System.out.println();

        check_all_after();

        // merge_two_nodes(1, 10);

        //System.out.println("节点本地时间为" + nodes[3].localTime + "    远程时间为" +nodes[3].remoteTime);
        //System.out.println("节点本地时间为" + nodes[4].localTime + "    远程时间为" +nodes[4].remoteTime);


    }

    private void end_merge() {
        boolean[] vis = new boolean[allNodeNum];
        for (int i = allNodeNum - 1; i >= 1; i --){
            int node_cluster_id = path_compress(i);
            for (int k = head[node_cluster_id]; k != -1; k = edges[k].next){
                int next_node_cluster = path_compress(edges[k].to);
                if (nodes[next_node_cluster].with_offload) {
                    continue;
                }

                //入度边权值占最大比重
                int cluster_i = path_compress(i);
                double res = edges[k].weight * in_edge_num[next_node_cluster] -
                        in_edge_total_value[next_node_cluster];
                if (edges[k].weight * in_edge_num[next_node_cluster] >=
                        in_edge_total_value[next_node_cluster]){ // 合并
                    double calculate_time = nodes[cluster_i].localTime + nodes[next_node_cluster].localTime -
                            nodes[cluster_i].remoteTime - nodes[next_node_cluster].remoteTime;
                    double message_time =out_edge_total_value[cluster_i] + in_edge_total_value[cluster_i] +
                            out_edge_total_value[next_node_cluster] + in_edge_total_value[next_node_cluster]
                            - edges[k].weight *2;
                    int offload = 0;
                    if (calculate_time - message_time - Const.OFFLOAD_VALUE > 0){
                        offload = 1;
                    }
                    end_merge_two_nodes(path_compress(i), next_node_cluster, offload);
                }
            }
        }
    }

    private void end_merge_two_nodes(Integer nodeaID, int nodebID, int offload) { // 两个节点都是族群的根节点
        System.out.println(" ====end join 的节点是" + nodeaID + "和节点 " + nodebID);
        if (offload == 1){
            nodes[nodeaID].with_offload = true;
        }

        int father_index = path_compress(nodeaID);


        nodes[father_index].remoteTime += nodes[nodebID].remoteTime;
        nodes[father_index].localTime += nodes[nodebID].localTime;

        in_edge_total_value[father_index] += in_edge_total_value[nodebID];
        out_edge_total_value[father_index] += out_edge_total_value[nodebID];
        in_edge_num[father_index] += in_edge_num[nodebID];
        out_edge_num[father_index] += out_edge_num[nodebID];

        

        int first_head = head[path_compress(nodeaID)];
        int second_head = head[path_compress(nodebID)];
        int res = remove_repeat(first_head, nodebID); // 节点a的边

        int res_second = remove_repeat(second_head, nodeaID); // 节点b去重后的边
        // 两条链表组合
        if (res == res_second && res == -1){
            head[nodeaID] = -1;
        }else  if (res == -1){
            head[nodeaID] = res_second;
        }else  if (res_second == -1) {
            head[nodeaID] = res;
        }else {
            int head_temp = res;
            int pre = head_temp;
            res = edges[res].next;
            while (res != -1){
                pre = res;
                res = edges[res].next;
            }
            edges[pre].next = res_second;
            head[nodeaID] = head_temp;
            //check(nodeaID, "end merge +++++++++after===============");
        }
        cluster[nodebID] = father_index; // 这里要最后赋值，否则会死循环
    }

    private Integer remove_repeat(Integer cur, Integer nodeID) {
        boolean is_head = false;
        int pre = cur;
        int head = pre;
        while (cur != -1){ // 头结点删除
            if (path_compress(edges[cur].to) == path_compress(nodeID)){
                cur = edges[cur].next;
            }else {
                break;
            }
        }
        if (cur == -1) { // 链表1全部删除
            return -1;
        }
        head = pre = cur;
        cur = edges[cur].next;
        while (cur != -1){
            if (path_compress(edges[cur].to) == path_compress(nodeID)){
                edges[pre].next = edges[cur].next;
                cur = edges[cur].next;
            } else {
                pre = cur;
                cur = edges[cur].next;
            }
        }
        return head;
       /* while (first_head != -1){ // 单指针直接扫描
            if (path_compress(edges[first_head].to) == nodeID){
                //直接指向后一个节点
                if (!is_head){ // 首节点删除
                    first_head = edges[first_head].next;
                    pre =  first_head;
                    head = pre; // 首节点
                    first_head = edges[first_head].next;
                }else {
                    edges[pre].next = edges[first_head].next;
                    pre =
                }
            }else {
                is_head = true;
                first_head = edges[first_head].next;
            }
        }*/
    }

    private void parallel_merge() { // 此处需要修改
        //第二轮， 分支合并
        for (int i = 1; i < allNodeNum; i ++){ //allNodeNum  如果顺利的话，后期的合并只需要新族内的点
            //if (path_compress(i) != i) continue; // 此处暂时先不加， 后期有其他情况
            if (out_edge_num[i] > 1){ // 先判断分支节点
                int temp_i = path_compress(i); //此时的节点i可能已经被之前的节点合并了
                double time_saving = nodes[temp_i].localTime - nodes[temp_i].remoteTime;
                double communication = out_edge_total_value[temp_i] + in_edge_total_value[temp_i];
                if (time_saving - communication < Const.OFFLOAD_VALUE){ //不适合单独卸载,需要合并
                    multi_input_unoffload(i);
                }else { //可以单独offload  可以根据边的具体情况考
                    multi_input_without_offload(i);
                }
            }
        }
    }

    void serial_merge(){
        boolean[] vis = new boolean[length];
        // 第一圈， 串行合并
        for (int k = 1; k <= allNodeNum; k ++) {
            //if (path_compress(k) != k) continue; //该节点已经被别的节点所合并, 不需要从该节点出发找其他边
            if (vis[k]) continue;
            if (in_edge_num[k] == out_edge_num[k] && in_edge_num[k] == 1) { //节点i是单入单出
                int[] needJoin = new int[allNodeNum];
                int cnt = 0;
                int edgeId = head[k];
                int nextNodeId = edges[edgeId].to;
                while (in_edge_num[nextNodeId] == out_edge_num[nextNodeId] && in_edge_num[nextNodeId] == 1) {
                    needJoin[cnt++] = nextNodeId;
                    vis[nextNodeId] = true;
                    nextNodeId = edges[head[nextNodeId]].to;
                }
                if (cnt <= 0) continue;
                for (int x = 0; x < cnt; x++) {
                    System.out.print(needJoin[x] + " ");
                }
                System.out.println();
                //重构
                for (int i = cnt - 1; i >= 0; i--) {
                    nodes[k].remoteTime += nodes[needJoin[i]].remoteTime;
                    nodes[k].localTime += nodes[needJoin[i]].localTime;
                    cluster[needJoin[i]] = path_compress(k);
                }
                Edge edge = edges[head[needJoin[cnt - 1]]];
                //先把边赋值  否则to先变（对应的节点改变了），其他的也会变
                edges[head[k]].weight = edge.weight;
                edges[head[k]].to = edge.to;
                out_edge_total_value[k] = out_edge_total_value[needJoin[cnt - 1]];
                //该节点本身不具备next
                //edges[head[edge.from]].next = -1;

                if (cnt > 0){
                    double time = nodes[k].localTime - nodes[k].remoteTime - out_edge_total_value[k] -
                            in_edge_total_value[k];
                    if (time - Const.OFFLOAD_VALUE > 0){
                        nodes[k].with_offload = true;
                    }
                }

                System.out.println(k + "源点到结束点 " + edge.to + "  权值 " +
                        edges[head[k]].weight);
                System.out.println("节点" + k + "本地执行时间为" + nodes[k].localTime + "  远程时间为"
                        + nodes[k].remoteTime);
            }
        }
    }
    void multi_input_without_offload(Integer i) {  // 改为其他族群节点
        double merge = -1D;
        int node = -1;
        for (int k = head[i]; k != -1; k = edges[k].next){
            int to = path_compress(edges[k].to);
            if (path_compress(to) == to && nodes[to].with_offload){
                continue;
            }
            double w = edges[k].weight/(in_edge_total_value[to] + out_edge_total_value[to]); // 该边于点to的权值比
            if (w > merge){
                merge = w;
                node = to;
            }
        }
        if (merge > Const.WEIGHT_RATE) merge_two_nodes(i, node, 1);
    }

    void multi_input_unoffload(Integer i){  // to节点id变为族群的id
        double more_max = Const.MAX;
        int merge_index = -1;
        int flag = 0;
        double max_income = - Const.MAX;
        double exe_time = 0D;
        int father_node = path_compress(i);


        for (int k = head[i]; k != -1; k = edges[k].next){
            int to = path_compress(edges[k].to);
            double calculate_time = nodes[father_node].localTime + nodes[to].localTime -
                    nodes[father_node].remoteTime - nodes[to].remoteTime;
            double message_time =out_edge_total_value[father_node] + in_edge_total_value[father_node] +
                    out_edge_total_value[to] + in_edge_total_value[to] - edges[k].weight *2;
            exe_time = calculate_time - message_time - Const.OFFLOAD_VALUE; // 大于0时，合并满足卸载状态
            //System.out.println("to为 " + to + " exe_time为 " + exe_time);
            if (exe_time > 0 && exe_time < more_max){ //最逼近offload临界状态
                more_max = exe_time;
                flag = 1;
                merge_index = to;
            }
            if ( (flag == 0) && (exe_time > max_income)){  // 不满足offload的条件就选择最大的
               // System.out.println("????  " + exe_time);
                max_income = exe_time;
                merge_index = to;
            }
        }

        System.out.println("合并的两个节点下标是 "+ i + " "  + merge_index +
                "==============最大的负值是 " + max_income );

        //将节点i和to合并    i是本节点，merge_index是族群根节点
        merge_two_nodes(i, merge_index, flag);

    }

    Integer path_compress(Integer pathx){
        if (cluster[pathx] == pathx) return pathx;
        return cluster[pathx] = path_compress(cluster[pathx]);
    }

    public void merge_two_nodes(Integer nodeaId, Integer nodebId, Integer flag){// 循环依赖的边也要判断
        //System.out.println("合并的两个节点**  " + nodeaId + "  " + nodebId);

        System.out.println("合并的节点是 " + nodeaId +"(" + path_compress(nodeaId) + ")和节点"
            + nodebId + "(" + path_compress(nodebId) +")");

        // 需要修改  path
        if (flag == 1) nodes[nodeaId].with_offload = true;

        int father_index = path_compress(nodeaId);
        nodes[father_index].remoteTime += nodes[nodebId].remoteTime;
        nodes[father_index].localTime += nodes[nodebId].localTime;
        cluster[nodebId] = father_index;

        in_edge_total_value[father_index] += in_edge_total_value[nodebId];
        out_edge_total_value[father_index] += out_edge_total_value[nodebId];
        in_edge_num[father_index] += in_edge_num[nodebId];
        out_edge_num[father_index] += out_edge_num[nodebId];  //  值都必须加到 最高几点上,还需修改
      /*  double in_edge_value = 0D;
        double out_edge_value = 0D;
        int local_in_edge = 0;
        int local_out_edge = 0;*/
        //更新入度出度边的数目以及出入度总权值


        // 第一个节点的边 变化
        int father_nodeaId = path_compress(nodeaId);


        int first = head[father_index]; // 边的下表

        int before_index = first;

        // nodea相关边中去掉跟Nodeb关联的边
        before_index = first_node_deal( first,  nodeaId,  nodebId,  before_index,  father_nodeaId);
        check(1, "--------after----");
       /*boolean the_head = false;
        while ( first != -1){
            if ( path_compress(edges[first].to) == path_compress(nodebId)){ //该边去掉
                System.out.println("去掉边" + edges[first].to);
                out_edge_num[father_nodeaId]--;
                in_edge_num[father_nodeaId]--;
                out_edge_total_value[father_nodeaId] -= edges[first].weight;
                in_edge_total_value[father_nodeaId] -= edges[first].weight;
                if (!the_head){ // 是第一条边
                    before_index = edges[first].next;
                    //head[nodeaId] = before_index; // 改变第一条边的位置
                    head[path_compress(nodeaId)] = before_index;
                    if (before_index == -1){ // 只有一条边, 且此时被删掉了

                        first = -1;
                    }else {
                        first = edges[before_index].next;
                    }

                }else {
                    edges[before_index].next = edges[first].next;
                    if (edges[first].next != -1){
                        before_index = edges[first].next; //可能此处first是最后一个节点
                    }
                    first = edges[first].next;
                }

            }else {
                the_head = true;
                before_index = first;   //
                first = edges[first].next;
                edges[before_index].next = first;
            }

        }
*/
        System.out.println("前段边变化结束");

        /*for (int x = head[nodeaId]; x != -1; x = edges[x].next){
            System.out.println("节点" + nodeaId + "的临界边是" + edges[x].to);
        }*/


/*
        for (int x = head[path_compress(nodebId)]; x != -1; x = edges[x].next){
            System.out.println("节点" + nodebId + "的临界边是" + edges[x].to);
        }*/


        //edges[before_index].next = head[nodebId]; //拼接
        //第二个节点的边变化
        first = head[nodebId];


        if (before_index == -1){ //第一个节点被合并后没有边
            head[path_compress(nodeaId)] = head[path_compress(nodebId)];
            before_index = first = head[path_compress(nodebId)];
            first_node_deal( first,  nodebId,  nodeaId,  before_index,  father_nodeaId);

        } else { // 合并完之后第一个节点还有边
            while (first != -1){
                int to = edges[first].to;

                if (path_compress(to) == path_compress(father_nodeaId)){
                    out_edge_num[father_nodeaId]--;
                    in_edge_num[father_nodeaId]--;
                    out_edge_total_value[father_nodeaId] -= edges[first].weight;
                    in_edge_total_value[father_nodeaId] -= edges[first].weight;

                    edges[before_index].next = edges[first].next;
                    before_index = first;
                } else { // 正常情况边衔接
                    edges[before_index].next = first;
                    before_index = first;
                }

                first = edges[first].next;
                // System.out.println("????????????//////////// " + edges[first].next);
            }
        }


       /* System.out.println("合并后的节点边数目");
        for (int x = head[nodeaId]; x != -1; x = edges[x].next){
            System.out.println("原边为 " + edges[x].from + "----->" + edges[x].to);
        }
        System.out.println("入度值和出度值分别是 " + in_edge_total_value[1] + "    " + out_edge_total_value[1]);
*/
        System.out.println("节点" +nodeaId + "   " + nodebId + "合并结束");
        check_all_after();
    }
    int first_node_deal(int first, int nodeaId, int nodebId, int before_index, int father_nodeaId){
        boolean the_head = false;
        //nodebId = path_compress(nodebId);
        while ( first != -1){
            if ( path_compress(edges[first].to) == path_compress(nodebId)){ //该边去掉
                System.out.println("去掉边" + edges[first].to);
                out_edge_num[father_nodeaId]--;
                in_edge_num[father_nodeaId]--;
                out_edge_total_value[father_nodeaId] -= edges[first].weight;
                in_edge_total_value[father_nodeaId] -= edges[first].weight;
                if (!the_head){ // 是第一条边
                    before_index = edges[first].next;
                    //head[nodeaId] = before_index; // 改变第一条边的位置
                    head[path_compress(nodeaId)] = before_index;
                    if (before_index == -1){ // 只有一条边, 且此时被删掉了

                        first = -1;
                    }else {
                        first = edges[before_index].next;
                    }

                }else {
                    edges[before_index].next = edges[first].next;
                    if (edges[first].next != -1){
                        before_index = edges[first].next; //可能此处first是最后一个节点
                    }
                    first = edges[first].next;
                }

            }else {
                the_head = true;
                before_index = first;   //
                first = edges[first].next;
                edges[before_index].next = first;
            }

        }
        return before_index;
    }
}
