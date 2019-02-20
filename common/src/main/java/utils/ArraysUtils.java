package utils;

/**
 * 数组工具类
 *
 * @author dengguoqing
 * @date 2019/2/20
 */
public class ArraysUtils {
    private ArraysUtils() {
    }

    public static void isEmpty(String[] args){
        if (null == args|| args.length ==0){
            throw new IllegalArgumentException("参数长度错误");
        }
    }
}
