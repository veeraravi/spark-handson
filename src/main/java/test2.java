import java.text.SimpleDateFormat;
import java.util.Date;

public class test2
{

    public static void main(String[] args) {
        try {
            String[] dates = {"2018-12-01", "2018-12-31", "2019-01-01"};
            for (String date: dates) {
                SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
                Date d = dt.parse(date);
                SimpleDateFormat dtYYYY = new SimpleDateFormat("YYYY");
                SimpleDateFormat dtyyyy = new SimpleDateFormat("yyyy");
                System.out.println("For date " + date + " the YYYY year is " + dtYYYY.format(d) + " while for yyyy it's " + dtyyyy.format(d));
            }
        } catch (Exception e) {
            System.out.println("Failed with exception: " + e);
        }
    }

}
