package utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TodayUtils {

    /**
     * YYYY-mm-DD
     *
     * @return
     */
    public static String getToday() {
        return LocalDate.now().toString();
    }

    /**
     * YYYYmmDD
     *
     * @return
     */
    public static String getTodayOnlyNumbers() {
        return LocalDate.now().toString().replace("-", "");
    }

    /**
     * YYYYmm
     *
     * @return
     */
    public static String getTodayOnlyYearMonth() {
        String year = String.valueOf(LocalDate.now().getYear());
        String month = String.valueOf(LocalDate.now().getMonthValue());
        if (month.length() == 1) {
            month = "0".concat(month);
        }
        return year.concat(month);
    }

    /**
     * yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
    public static String getTodayWithHours() {
        return LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .replace(" ", "T")
                .replace(":", "")
                .replace("-", "");
    }
}