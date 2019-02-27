package average;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class SimpleMovingAverageTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SimpleMovingAverageTest.class);
    @Test
    public void getMovingAverage() {
        double[] testData = {10, 18, 20, 31, 24, 33, 27};
        int[] allWindowSizes = {3, 4};
        for (int allWindowSize : allWindowSizes) {
            SimpleMovingAverage sma = new SimpleMovingAverage(allWindowSize);
            LOGGER.info("windowSize = {}", allWindowSize);
            for (double data : testData) {
                sma.addNewNumber(data);
                LOGGER.info("Next Number = {},SMA = {}", data, sma.getMovingAverage());
            }
            LOGGER.info("-------");
        }
    }
}