import org.apache.hadoop.io.Text;

/**
 * Created by Arpit on 2/20/2015.
 */
public class GSODTempParser {
    private static final int MISSING_TEMPERATURE = 99999;
    private String year;
    private float airTemperature;

    public void parse(String record) {
        year = record.substring(14, 18);
        String airTemperatureString = record.substring(102, 108);
        airTemperature = Float.parseFloat(airTemperatureString.trim());
    }

    public void parse(Text record) { parse(record.toString());}

    public boolean isValidTemperature() { return airTemperature != MISSING_TEMPERATURE;}

    public String getYear() { return year;}

    public float getAirTemperature(){ return airTemperature;}



}
