package enu;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/30
 */
public class EnumTest {

    enum TestCount{
        OVER
    }

    public static void main(String[] args) {
        TestCount.OVER.ordinal();
    }
}
