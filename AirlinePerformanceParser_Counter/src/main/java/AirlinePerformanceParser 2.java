import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {

    private int year; // 운항 연도
    private int month; // 운항 월

    private int arriveDelayTime = 0; // 항공기 도착 지연 시간
    private int departureDelayTime = 0; // 항공기 출발 지연 시간
    private int distance = 0; // 거리

    private boolean arriveDelayAvailable = true;
    private boolean departureDelayAvailable = true;
    private boolean distanceAvailable = true;

    private String uniqueCarrier; // 항공사 코드

    public AirlinePerformanceParser(Text text) {

        try {
            String[] colums = text.toString().split(","); // csv 파일은 ,를 구분자로 사용하기 때문에 이를 분리한다.

            year = Integer.parseInt(colums[0]); // 운항 연도 설정
            month = Integer.parseInt(colums[1]); // 운항 월 설정
            uniqueCarrier = colums[8]; // 항공사 코드 설정

            if(!colums[15].equals("NA")) // 출발 지연 시간 설정
                departureDelayTime = Integer.parseInt(colums[15]);
            else
                departureDelayAvailable = false;

            if(!colums[14].equals("NA")) // 도착 지연 시간 설정
                arriveDelayTime = Integer.parseInt(colums[14]);
            else
                arriveDelayAvailable = false;

            if(!colums[18].equals("NA"))
                distance = Integer.parseInt(colums[18]);
            else
                distanceAvailable = false;
            // 출발 지연 시간과 도착 지연 시간, 거리에는 NA값이 들어가 있는 경우가 있으므로, Exception을 미리 방지하기 위해 위 조건문처럼 처리하도록 한다.
        }
        catch (Exception e) {
            System.out.println("Error parsing a record : " + e.getMessage());
        }
    }

    public int getYear() { return year; }
    public int getMonth() { return month; }
    public int getArriveDelayTime() { return arriveDelayTime; }
    public int getDepartureDelayTime() { return departureDelayTime; }

    public boolean isArriveDelayAvailable() { return arriveDelayAvailable; }
    public boolean isDepartureDelayAvailable() { return departureDelayAvailable; }

    public String getUniqueCarrier() { return uniqueCarrier; }

    public int getDistance() { return distance; }

    public boolean isDistanceAvailable() { return distanceAvailable; }
}
