package tech.powerjob.server.test;

import tech.powerjob.common.OmsConstant;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.TimeZone;

/**
 * 时区问题测试
 *
 * @author tjq
 * @since 2020/6/24
 */
public class TimezoneTest {

    @Test
    public void testTimeZone() {
        Date now = new Date();
        System.out.println(now.toString());

        System.out.println("timestamp before GMT: " + System.currentTimeMillis());

        // 保存并恢复默认时区，避免污染同 JVM 中其他时间敏感测试
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            TimeZone timeZone = TimeZone.getDefault();
            System.out.println(timeZone.getDisplayName());
            System.out.println(new Date());
            System.out.println(DateFormatUtils.format(new Date(), OmsConstant.TIME_PATTERN));

            System.out.println("timestamp after GMT: " + System.currentTimeMillis());
        } finally {
            TimeZone.setDefault(original);
        }
    }

}
