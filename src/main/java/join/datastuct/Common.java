package main.java.join.datastuct;

public class Common {
    public static Boolean isEqual(Double a, Double b){
        return (a - b > -1e-6)&&(a - b < 1e-6);
    }
}
