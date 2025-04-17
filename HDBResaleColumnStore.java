import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.*;
import java.time.YearMonth;
import java.util.*;
import java.util.stream.Collectors;

class HDBResaleColumnStore {
    private static final String DATA_DIR = "column_store";
    private static final String OUTPUTCSV = "output/SortedResalePrices.csv";
    private static final Map<String, List<Long>> FILEINDICES = new HashMap<>();

    private static void ensureDirectoriesExist() {
        String[] directories = {"output", "column_store"};
        for (String dir : directories) {
            File directory = new File(dir);
            if (!directory.exists()) {
                if (directory.mkdirs()) {
                    System.out.println("Directory '" + dir + "' created.");
                } else {
                    System.out.println("Failed to create directory '" + dir + "'.");
                }
            }
        }
    }

    //// Preprocessing 
    // This function sorts the CSV file by month and writes it to a new file
    public static void sortCSVByMonth(String inputCsvPath, String outputCsvPath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(inputCsvPath));
        if (lines.isEmpty()) return;
        
        String header = lines.get(0);
        String[] headers = header.split(",");
        // Find the index of the month column (assuming the header contains "month")
        int monthIndex = -1;
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase("month")) {
                monthIndex = i;
                break;
            }
        }
        if (monthIndex == -1) {
            throw new RuntimeException("Month column not found");
        }
        
        List<String[]> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            rows.add(lines.get(i).split(","));
        }
        
        final int finalMonthIndex = monthIndex;
        // Sort rows based on the month column using YearMonth
        rows.sort(Comparator.comparing(row -> YearMonth.parse(row[finalMonthIndex])));
        
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputCsvPath))) {
            writer.write(header);
            writer.newLine();
            for (String[] row : rows) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        }
        System.out.println("Sorted CSV written to " + outputCsvPath);
    }

    public static void MultiFileCSVAccess() throws IOException {
        
        buildIndexTable("column_store/month.csv");
        buildIndexTable("column_store/town.csv");
        buildIndexTable("column_store/floor_area_sqm.csv");
        buildIndexTable("column_store/resale_price.csv");
    }

    // Build index look up table
    public static void buildIndexTable(String filePath) throws IOException {
    List<Long> offsets = new ArrayList<>();
    try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
        long offset;
        while ((offset = raf.getFilePointer()) < raf.length()) {//
            offsets.add(offset); // Store the current file pointer position
            raf.readLine(); 
        }
    }
    FILEINDICES.put(filePath, offsets);
}


    //Get data directly from the CSV file using look up table
    /*public static String getLine(String filePath, int lineNumber) throws IOException {
        if(!FILEINDICES.containsKey(filePath)) {
            throw new IllegalArgumentException("File does not have an index look up table: " + filePath);
        }

        List<Long> offsets = FILEINDICES.get(filePath);
        if(lineNumber < 0 || lineNumber >= offsets.size()) {
            // throw new IndexOutOfBoundsException("Invalid line number for file : " + filePath);
            return null;
        }

        try(RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(offsets.get(lineNumber));
            
            return raf.readLine();
        }
    }*/  

    // Split the CSV file into separate files for each column in column_store directory
    public static void splitCSV(String csvPath) throws IOException {
        Files.createDirectories(Paths.get(DATA_DIR));

        BufferedReader reader = new BufferedReader(new FileReader(csvPath));
        String header = reader.readLine(); // Read header
        String[] columns = header.split(",");

        Map<String, BufferedWriter> writers = new HashMap<>();
        for (String col : columns) {
            writers.put(col, new BufferedWriter(new FileWriter(DATA_DIR + "/" + col + ".csv")));
        }
        int lineNumber = 1;
        String line;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            String[] values = line.split(",");
           
            if (values.length < columns.length) {// Check if the number of values is less than the number of columns. This is to include the last column after regex
                values = Arrays.copyOf(values, columns.length); // Extend the array
                for (int i = 0; i < columns.length; i++) {
                    if (values[i] == null || values[i].trim().isEmpty()) {
                        values[i] = "na"; // Fill missing values with "na"
                    }
                }
            }

            for (int i = 0; i < values.length; i++) {
                String colName = columns[i];
                String rawValue = values[i].trim();
                String value = ( "".equals(rawValue) || rawValue.isEmpty() || rawValue == null ) ? "na" : rawValue; //fills empty cells with "na" to indicate missing data

                // Data Type checking for the relevant column for computing output statistics
                if (value.equals("na")) {
                    System.out.println("WARNING: Empty cell in column '" + colName + "' at line " + lineNumber);
                } else {
                    if (colName.equalsIgnoreCase("month") && !value.matches("\\d{4}-\\d{2}")) {
                        System.out.println("WARNING: Invalid month format at line " + lineNumber + ": " + value);
                        value = "na";
                    }
                    if (colName.equalsIgnoreCase("town") && !value.matches("[A-Z /]+")) {
                        System.out.println("WARNING: Invalid town format at line " + lineNumber + ": " + value);
                        value = "na";
                    }
        
                    
                    if (colName.equalsIgnoreCase("floor_area_sqm") && !isNumeric(value)) {
                        System.out.println("WARNING: Invalid floor area at line " + lineNumber + ": " + value);
                        value = "na";
                    }            
                    if ((colName.equalsIgnoreCase("resale_price") || colName.equalsIgnoreCase("floor_area_sqm")) &&
                        !isNumeric(value)) {
                        System.out.println("WARNING: Invalid number in column '" + colName + "' at line " + lineNumber + ": " + value);
                        value = "na";
                    }
                }

                writers.get(colName).write(value + "\n");
            }
        }

        reader.close();
        for (BufferedWriter writer : writers.values()) {
            writer.close();
        }

        System.out.println("CSV split into column files in '" + DATA_DIR + "' directory.");
    }

    // Helper method to check if a string is numeric
    private static boolean isNumeric(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Generate zones based on the month file for zonemapping
    public static Map<String, Map<String, Integer>> generateZones() throws IOException {
        Map<String, Map<String, Integer>> zones = new HashMap<>();
        Path monthFilePath = Paths.get(DATA_DIR, "month.csv");
    
        if (!Files.exists(monthFilePath)) {
            System.out.println("Month file does not exist.");
            return zones;
        }
    
        List<String> lines = Files.readAllLines(monthFilePath);
        
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.length() >= 4 && line.substring(0, 4).matches("\\d{4}")) {
                String yearKey = line.substring(0, 4);
    
                zones.putIfAbsent(yearKey, new HashMap<>());
                zones.get(yearKey).putIfAbsent("start", i); // Set start index only once
                zones.get(yearKey).put("end", i);  // Always update end index
    
                // Debugging print statement
               
            }
        }
        // System.out.println("Zone: " + zones);
        return zones;
    }
    


    //// QUERIES
    // Normal Query
    public static void normalQuery(int year, int startMonth, String town) throws IOException {///////
        long startTime = System.currentTimeMillis();

        List<String[]> filteredData = normalScan(year, startMonth, town, 0, Integer.MAX_VALUE);

        long endTime = System.currentTimeMillis();
        System.out.println("Query Time: " + (endTime - startTime) + " ms");
        Map<String, Double> stats = computeStatistics(filteredData);
        writeStatisticsToCSV(stats, year, startMonth, town, "output/NormalQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }
    // Zone Mapping Query
    public static void zmQuery(int year, int startMonth, String town, Map<String, Map<String, Integer>> zones) throws IOException {
        
        String yearKey = String.valueOf(year);
        if (!zones.containsKey(yearKey)) {
            return;
        }
        long startTime = System.currentTimeMillis();
    List<String[]> filteredData = normalScan(year, startMonth, town, zones.get(yearKey).get("start"), zones.get(yearKey).get("end"));//
    long endTime = System.currentTimeMillis();
    System.out.println("Query Time: " + (endTime - startTime) + " ms");
    Map<String, Double> stats = computeStatistics(filteredData);
    writeStatisticsToCSV(stats, year, startMonth, town, "output/ZMQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }
    // Shared Scan Query
    public static void ssQuery(int year, int startMonth, String town) throws IOException { ////////
    long startTime = System.currentTimeMillis();
    List<String[]> filteredData = sharedScan(year, startMonth, town, 0, Integer.MAX_VALUE);
    long endTime = System.currentTimeMillis();
    System.out.println("Query Time: " + (endTime - startTime) + " ms");
    Map<String, Double> stats = computeStatistics(filteredData);
    writeStatisticsToCSV(stats, year, startMonth, town, "output/SSQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }
    // Zone Mapping + Shared Scan Query
    public static void zmssQuery(int year, int startMonth, String town, Map<String, Map<String, Integer>> zones) throws IOException {
        String yearKey = String.valueOf(year);
        if (!zones.containsKey(yearKey)) {
            return;
        }
    long startTime = System.currentTimeMillis();
    List<String[]> filteredData = sharedScan(year, startMonth, town, zones.get(yearKey).get("start"), zones.get(yearKey).get("end"));
    long endTime = System.currentTimeMillis();
    System.out.println("Query Time: " + (endTime - startTime) + " ms");
    Map<String, Double> stats = computeStatistics(filteredData);
    writeStatisticsToCSV(stats, year, startMonth, town, "output/ZMSSQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }



// Query helper functions for recyclability 
// As per lecture, the normal scan performs multi-stage filter on the data based on the year, month, town, and area. 
private static List<String[]> normalScan(int year, int startMonth, String town, int zone_startIdx, int zone_endIdx) throws IOException {

    BufferedReader monthReader = new BufferedReader(new FileReader(DATA_DIR + "/month.csv"));

    List<Integer> pos = new ArrayList<>();
    String monthLine;
    // Stage 1: Time filter using BufferedReader

    int index = 0;
    if(zone_startIdx != 0){
        index = zone_startIdx;
    }
    int adjustedEnd = zone_endIdx == Integer.MAX_VALUE ? Integer.MAX_VALUE : zone_endIdx + 1; // if zone_endIdx is not set (for non-zone index queries), set it to the size of the months list to perform a full column scan


    if(zone_endIdx != Integer.MAX_VALUE){// for zone mapping query, use the index to seek to the start of the file and read from there until zone mapping end index
        RandomAccessFile raf_month = new RandomAccessFile("column_store/month.csv", "r");
        if (!FILEINDICES.containsKey("column_store/month.csv")) {
            throw new IllegalArgumentException("File does not have an index look up table: column_store/month.csv");
        }
        raf_month.seek(FILEINDICES.get("column_store/month.csv").get(zone_startIdx));
        BufferedReader monthReader2 = new BufferedReader(new InputStreamReader(Channels.newInputStream(raf_month.getChannel()))); // use channel to wrap the raf reader for buffered read for faster access 

        String monthLine2;
        while ((monthLine2 = monthReader2.readLine()) != null && index < adjustedEnd) {
            if (index >= zone_startIdx && index < adjustedEnd) {
                if(monthLine2.equals("na")){
                    System.out.println("Error: Month Column contains anomalies...");
                    System.exit(0);
                }
                int monthValue = Integer.parseInt(monthLine2.substring(5, 7));
                int yearValue = Integer.parseInt(monthLine2.substring(0, 4));
                if (yearValue == year && (monthValue == startMonth || monthValue == (startMonth + 1))) {
                    pos.add(index); // add index to the list of positions that meet the query condition for month
                }
            }
            index++;
        }
        monthReader2.close();
        raf_month.close();

    }
    else{ // for normal scan, read the entire file using buffered reader for better efficiency
        while ((monthLine = monthReader.readLine()) != null ){

            if (index >= zone_startIdx && index < adjustedEnd) {
                // String monthLine = getLine("column_store/month.csv",index+1);
                // if(index == monthSize) break; //end of reading month column //11
                if(monthLine.equals("na")){
                    System.out.println("Error: Month Column contains anomalies...");
                    System.exit(0);
                }
                // System.err.println("index: " + index + "month size: " + monthSize + "monthline: " + monthLine);
                int monthValue = Integer.parseInt(monthLine.substring(5, 7));
                // System.err.println("index2: " + index + "month size2: " + monthSize + "monthline2: " + monthLine);
                int yearValue = Integer.parseInt(monthLine.substring(0, 4));
                if (yearValue == year && (monthValue == startMonth || monthValue == (startMonth + 1))) {
                    
                    pos.add(index); // add index to the list of positions that meet the query condition for month
                    // narrow down the read range for the next stage
                }
            }
            index++;    
        }

        monthReader.close();
    }
    
    // Stage 2: Town filter
    List<Integer> townPos = new ArrayList<>();
    String townFilePath = "column_store/town.csv";
    if(!FILEINDICES.containsKey(townFilePath)) {
        throw new IllegalArgumentException("File does not have an index look up table: " + townFilePath);
    }
    RandomAccessFile raf_town = new RandomAccessFile(townFilePath, "r");
    String townLine;
    for(int town_index: pos){
        // townLine = getLine("column_store/town.csv",town_index+1);
        raf_town.seek(FILEINDICES.get(townFilePath).get(town_index)); // Seek to the index of the month column
        townLine = raf_town.readLine(); // Read the line at the current file pointer
        if (townLine.equalsIgnoreCase("na")) {
            System.out.println("Error: Town Column contains anomalies...");
            System.exit(0);
        }
        if (townLine.equalsIgnoreCase(town)) {
            townPos.add(town_index);
        } 
        
    }
    raf_town.close();
    // Stage 3: Area filter
    List<Integer> areaPos = new ArrayList<>();
    String areaFilePath = "column_store/floor_area_sqm.csv";
    if(!FILEINDICES.containsKey(areaFilePath)) {
        throw new IllegalArgumentException("File does not have an index look up table: " + areaFilePath);
    }
    RandomAccessFile raf_area = new RandomAccessFile(areaFilePath, "r");

    String areaLine;
    for(int area_index : townPos){
        // areaLine = getLine("column_store/floor_area_sqm.csv",area_index+1);
        raf_area.seek(FILEINDICES.get(areaFilePath).get(area_index)); // Seek to the index of the month column
        areaLine = raf_area.readLine(); // Read the line at the current file pointer
        if (areaLine.equals("na")) {
            System.out.println("Error: Floor Area Column contains anomalies...");
            System.exit(0);
        }
        if (Double.parseDouble(areaLine) >= 80) {
            areaPos.add(area_index);
        }
        
    }

    // Final: Fetch prices and areas for filtered positions
    List<String[]> filtered = new ArrayList<>();
    String priceFilePath = "column_store/resale_price.csv";
    String priceLine, areaAgainLine;
    Set<Integer> lookup = new HashSet<>(areaPos);
    if(!FILEINDICES.containsKey(priceFilePath)) {
        throw new IllegalArgumentException("File does not have an index look up table: " + priceFilePath);
    }
    RandomAccessFile raf_price = new RandomAccessFile(priceFilePath, "r");
    for(int finalIndex : areaPos){
        // priceLine = getLine("column_store/resale_price.csv",finalIndex+1);
        // areaAgainLine = getLine("column_store/floor_area_sqm.csv",finalIndex+1);
        raf_price.seek(FILEINDICES.get(priceFilePath).get(finalIndex)); // Seek to the index of the month column
        priceLine = raf_price.readLine(); // Read the line at the current file pointer
        raf_area.seek(FILEINDICES.get(areaFilePath).get(finalIndex)); // Seek to the index of the month column
        areaAgainLine = raf_area.readLine(); // Read the line at the current file pointer

        if (priceLine.equals("na")) {
            System.out.println("Error: Resale price column contains anomalies...");
            System.exit(0);
        }
        if (lookup.contains(finalIndex)) {
            if (priceLine.equals("na") || areaAgainLine.equals("na")) {
                System.out.println("Error: Resale price or floor area column contains anomalies...");
                System.exit(0);
            }
            filtered.add(new String[]{priceLine, areaAgainLine});
        }
 
    }
    raf_price.close();
    raf_area.close();
    return filtered;
}



// As per lecture, the shared scan performs a one-pass filter on the data based on the year, month, town, and area 
private static List<String[]> sharedScan(int year, int startMonth, String town, int zone_startIdx, int zone_endIdx) throws IOException {

    List<String[]> filtered = new ArrayList<>();
    int index = 0;
    int adjustedEnd = zone_endIdx == Integer.MAX_VALUE ? Integer.MAX_VALUE : zone_endIdx + 1; // if zone_endIdx is not set (for non-zone index queries), set it to the size of the months list to perform a full column scan

    if(adjustedEnd == Integer.MAX_VALUE){ //for normal scan, read the entire file using buffered reader for better efficiency
    String monthLine, townLine, areaLine, priceLine;
    BufferedReader monthReader = new BufferedReader(new FileReader(DATA_DIR + "/month.csv"));
    BufferedReader townReader = new BufferedReader(new FileReader(DATA_DIR + "/town.csv"));
    BufferedReader areaReader = new BufferedReader(new FileReader(DATA_DIR + "/floor_area_sqm.csv"));
    BufferedReader priceReader = new BufferedReader(new FileReader(DATA_DIR + "/resale_price.csv"));
  
        while((monthLine = monthReader.readLine()) != null &&
            (townLine = townReader.readLine()) != null &&
            (areaLine = areaReader.readLine()) != null &&
            (priceLine = priceReader.readLine()) != null) {

            if (index >= zone_startIdx && index < adjustedEnd) {
                int monthValue = Integer.parseInt(monthLine.substring(5, 7));
                int yearValue = Integer.parseInt(monthLine.substring(0, 4));
                if (monthLine.equals("na") || townLine.equals("na") || areaLine.equals("na") || priceLine.equals("na")) { // null or wrong data type check
                    System.out.println("Error: Month, Town, Floor Area or Resale Price Column contains anomalies for the selected year and month. Please check initial warning and ResalePricesSingapore.csv file.");
                    System.exit(0);
                }
                if ((yearValue == year) && (monthValue == startMonth || monthValue == (startMonth + 1)) &&
                    townLine.equalsIgnoreCase(town) && Double.parseDouble(areaLine) >= 80) {
                    filtered.add(new String[]{priceLine, areaLine});

                }
            }
            index++;
        }
    }
    else{ // zone mapping query: use the index to seek to the start of the file and read from there until zone mapping end index
 
     // File paths
    String monthFilePath = "column_store/month.csv";
    String townFilePath = "column_store/town.csv";
    String areaFilePath = "column_store/floor_area_sqm.csv";
    String priceFilePath = "column_store/resale_price.csv";

 // Seek once to zone_startIdx, wrap BufferedReader for buffered sequential read 
    RandomAccessFile raf_month = new RandomAccessFile(monthFilePath, "r");
    raf_month.seek(FILEINDICES.get(monthFilePath).get(zone_startIdx));
    BufferedReader monthReader = new BufferedReader(new InputStreamReader(Channels.newInputStream(raf_month.getChannel()))); // use channel to wrap the raf reader for buffered read for faster access 

    RandomAccessFile raf_town = new RandomAccessFile(townFilePath, "r");
    raf_town.seek(FILEINDICES.get(townFilePath).get(zone_startIdx));
    BufferedReader townReader = new BufferedReader(new InputStreamReader(Channels.newInputStream(raf_town.getChannel())));

    RandomAccessFile raf_area = new RandomAccessFile(areaFilePath, "r");
    raf_area.seek(FILEINDICES.get(areaFilePath).get(zone_startIdx));
    BufferedReader areaReader = new BufferedReader(new InputStreamReader(Channels.newInputStream(raf_area.getChannel())));

    RandomAccessFile raf_price = new RandomAccessFile(priceFilePath, "r");
    raf_price.seek(FILEINDICES.get(priceFilePath).get(zone_startIdx));
    BufferedReader priceReader = new BufferedReader(new InputStreamReader(Channels.newInputStream(raf_price.getChannel())));

    // Sequential scan from zone_startIdx up to zone_endIdx
    int linesToRead = zone_endIdx - zone_startIdx;
    for (int i = 0; i < linesToRead; i++) {
        String monthLine = monthReader.readLine();
        String townLine = townReader.readLine();
        String areaLine = areaReader.readLine();
        String priceLine = priceReader.readLine();

        if (monthLine == null || townLine == null || areaLine == null || priceLine == null) break;

        // Basic anomaly check
        if (monthLine.equals("na") || townLine.equals("na") || areaLine.equals("na") || priceLine.equals("na")) {
            System.out.println("Error: Column contains anomalies. Please check ResalePricesSingapore.csv.");
            System.exit(0);
        }

        int monthValue = Integer.parseInt(monthLine.substring(5, 7));
        int yearValue = Integer.parseInt(monthLine.substring(0, 4));

        if ((yearValue == year) &&
            (monthValue == startMonth || monthValue == (startMonth + 1)) &&
            townLine.equalsIgnoreCase(town) &&
            Double.parseDouble(areaLine) >= 80) {
            filtered.add(new String[]{priceLine, areaLine});
        }
    }
    monthReader.close();
    townReader.close();
    areaReader.close();
    priceReader.close();

    }

    return filtered;

}

    
    // Compute statistics on the filtered data
    public static Map<String, Double> computeStatistics(List<String[]> filteredData) {
        if (filteredData.isEmpty()) {
            return Map.of(
                "Minimum Price", -1.0,
                "Average Price", -1.0,
                "Standard Deviation of Price", -1.0,
                "Minimum Price per Square Meter", -1.0
            );
        }
    

        List<Double> prices = filteredData.stream().map(row -> Double.parseDouble(row[0])).collect(Collectors.toList());
        List<Double> pricePerSqm = filteredData.stream().map(row -> Double.parseDouble(row[0]) / Double.parseDouble(row[1])).collect(Collectors.toList());
        

        double minPrice = Collections.min(prices);
        double avgPrice = prices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double stdDev = computeStandardDeviation(prices, avgPrice);
        double minPricePerSqm = Collections.min(pricePerSqm);
        
        return Map.of(
                "Minimum Price", minPrice,
                "Average Price", avgPrice,
                "Standard Deviation of Price", stdDev,
                "Minimum Price per Square Meter", minPricePerSqm
        );
    }

    // Compute standard deviation
    private static double computeStandardDeviation(List<Double> values, double mean) {
        if (values.size() <= 1) return 0.0;
        double variance = values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).sum() / (values.size()-1);
        return Math.sqrt(variance);
    }
    
        
    private static void writeStatisticsToCSV(Map<String, Double> stats, int year, int startMonth, String town, String outputFile) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("Year,Month,Town,Category,Value\n\n");
            System.out.println("\nResults for " + "Year: " + year + ", " + "Month: " + startMonth + ", " + "Town: "+ town);
            System.out.println("=================================================");
            for (var entry : stats.entrySet()) {
            writer.write(year + "," + startMonth + "," + town + "," + entry.getKey() + "," + String.format("%.2f", entry.getValue()) + "\n");
            System.out.println(entry.getKey() + " = " + String.format("%.2f", entry.getValue()));
               
            }
        }
        // System.out.println("Results saved to " + outputFile);
        }

    // Extract matriculation number to get year, month, and town
    private static QueryParams matricExtraction(String userInput) {
        int n = userInput.length();
        int year = userInput.charAt(n - 2) - '0';
        int month = userInput.charAt(n - 3) - '0';
        int town = userInput.charAt(n - 4) - '0';
    
        Map<Integer, String> townConverter = Map.of(
            0, "BEDOK", 1, "BUKIT PANJANG", 2, "CLEMENTI", 3, "CHOA CHU KANG", 4, "HOUGANG", 5, "JURONG WEST", 6, "PASIR RIS", 7, "TAMPINES",
            8, "WOODLANDS", 9, "YISHUN"
        );
    
        Map<Integer, Integer> monthConverter = Map.of(
            0, 10, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9
        );
    
        Map<Integer, Integer> yearConverter = Map.of(
            0, 2020, 1, 2021, 2, 2022, 3, 2023, 4, 2014, 5, 2015, 6, 2016, 7, 2017, 8, 2018, 9, 2019
        );
    
        return new QueryParams(
            yearConverter.getOrDefault(year, 2020),
            monthConverter.getOrDefault(month, 1),
            townConverter.getOrDefault(town, "BEDOK")
        );
    }
    
    record QueryParams(int year, int month, String town) {} //simple record to hold query parameters
    


    //// Main, to run the queries
    public static void main(String[] args) throws IOException {
        String csvPath = "ResalePricesSingapore.csv";
        ensureDirectoriesExist();

        // One-time preprocessing step
        sortCSVByMonth(csvPath, OUTPUTCSV);
        splitCSV(OUTPUTCSV); 
        MultiFileCSVAccess();


        // Generate zones for zone mapping
        Map<String, Map<String, Integer>> zones = generateZones();

        try (Scanner userInput = new Scanner(System.in)) {
            // Prompt user for matriculation number
            System.out.println("Enter Matriculation No.");
            String matricNo = userInput.nextLine();  

            // Extract year, month, and town from matriculation number
            QueryParams params = matricExtraction(matricNo);
            int year = params.year();
            int startMonth = params.month();
            String town = params.town();


            //// Uncomment the following to run and compare queries performance. Output result will be saved to `output` folder////
        
            // Normal Query
            System.out.println("\nRunning Normal Query...");
            normalQuery(year, startMonth, town);

            // Zone Mapping Query
            System.out.println("\nRunning Zone Mapping Query...");
            zmQuery(year, startMonth, town, zones);

            // Shared Scan Query
            System.out.println("\nRunning Shared Scan Query...");
            ssQuery(year, startMonth, town);
            
            // Zone Mapping + Shared Scan Query
            System.out.println("\nRunning Zone Mapping + Shared Scan Query...");
            zmssQuery(year, startMonth, town, zones);

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
        
    }
}
