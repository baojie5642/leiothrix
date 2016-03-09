package xin.bluesky.leiothrix.common.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 *
 * @author 张轲
 */
public class DateUtils2 {

    protected static final String FORMAT_FULL = ("yyyy-MM-dd HH:mm:ss");

    protected static final String FORMAT_MEDIUM = ("yyyy-MM-dd");

    /**
     * 将long型的时间戳转化为字符串(格式为:年-月-日 时:分:秒)
     *
     * @param time
     * @return
     */
    public static String formatFull(long time) {
        return new SimpleDateFormat(FORMAT_FULL).format(new Date(time));
    }

    /**
     * 将日期转化为字符串
     *
     * @param date
     * @return
     */
    public static String formatFull(Date date) {
        if (date == null) {
            return null;
        }
        return new SimpleDateFormat(FORMAT_FULL).format(date);
    }

    /**
     * 将日期转化为字符串
     *
     * @param date
     * @return
     */
    public static String formatMedium(Date date) {
        if (date == null) {
            return null;
        }
        return new SimpleDateFormat(FORMAT_MEDIUM).format(date);
    }

    /**
     * 将日期转化为时间戳,并以字符串形式返回.如果日期为空,则返回空字符串.
     *
     * @param date
     * @return
     */
    public static String toTime(Date date) {
        if (date == null) {
            return "";
        }

        return String.valueOf(date.getTime());
    }
}
