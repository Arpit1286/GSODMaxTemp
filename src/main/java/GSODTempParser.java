import org.apache.hadoop.io.Text;


public class GSODTempParser {
    private static final float MISSING_TEMPERATURE = (float) 9999.9;
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
