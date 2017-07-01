package wlw;

import java.text.DecimalFormat;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        int a=1099;
        int b=93;
        DecimalFormat df = new DecimalFormat("0.0000");//格式化小数
        String num = df.format((float)a/b);//返回的是String类型
        System.out.println("ddd==="+num);


    }
}
