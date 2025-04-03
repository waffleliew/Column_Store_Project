import java.io.*;
import java.nio.file.*;
import java.time.YearMonth;
import java.util.*;
import java.util.stream.Collectors;

class HDBResaleColumnStore {
    private static final String DATA_DIR = "column_store";
    private static final String OUTPUTCSV = "output/SortedResalePrices.csv";


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
        
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            for (int i = 0; i < values.length; i++) {
                writers.get(columns[i]).write(values[i] + "\n");
            }
        }
        
        reader.close();
        for (BufferedWriter writer : writers.values()) {
            writer.close();
        }
        System.out.println("CSV split into column files in '" + DATA_DIR + "' directory.");
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
        List<String[]> filteredData = normalScan(year, startMonth, town, 0, Integer.MAX_VALUE);
        Map<String, Double> stats = computeStatistics(filteredData);
        writeStatisticsToCSV(stats, year, startMonth, town, "output/NormalQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }
    // Zone Mapping Query
    public static void zmQuery(int year, int startMonth, String town, Map<String, Map<String, Integer>> zones) throws IOException {
    String yearKey = String.valueOf(year);
    if (!zones.containsKey(yearKey)) {
        return;
    }
    List<String[]> filteredData = normalScan(year, startMonth, town, zones.get(yearKey).get("start"), zones.get(yearKey).get("end"));//
    Map<String, Double> stats = computeStatistics(filteredData);
    writeStatisticsToCSV(stats, year, startMonth, town, "output/ZMQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }
    // Shared Scan Query
    public static void ssQuery(int year, int startMonth, String town) throws IOException { ////////
    List<String[]> filteredData = sharedScan(year, startMonth, town, 0, Integer.MAX_VALUE);
    Map<String, Double> stats = computeStatistics(filteredData);
    writeStatisticsToCSV(stats, year, startMonth, town, "output/SSQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }
    // Zone Mapping + Shared Scan Query
    public static void zmssQuery(int year, int startMonth, String town, Map<String, Map<String, Integer>> zones) throws IOException {
    String yearKey = String.valueOf(year);
    if (!zones.containsKey(yearKey)) {
        return;
    }
    List<String[]> filteredData = sharedScan(year, startMonth, town, zones.get(yearKey).get("start"), zones.get(yearKey).get("end"));
    Map<String, Double> stats = computeStatistics(filteredData);
    writeStatisticsToCSV(stats, year, startMonth, town, "output/ZMSSQuery_" + year + "_" + startMonth + "_" + town + ".csv");
    }



    //// Query helper functions for recyclability 
    // As per lecture, the normal scan performs multi-stage filter on the data based on the year, month, town, and area
    private static List<String[]> normalScan(int year, int startMonth, String town, int zone_startIdx, int zone_endIdx) throws IOException {
        List<String> months = Files.readAllLines(Paths.get(DATA_DIR, "month.csv"));
        List<Integer> pos = new ArrayList<>();
    
        if (zone_endIdx == Integer.MAX_VALUE) { // If zone_endIdx is not set (for non-zone index queries), set it to the size of the months list to perform a full column scan
            zone_endIdx = months.size();
        } else {
            zone_endIdx += 1;
        }
    
        // Stage 1: Time filter
        for (int i = zone_startIdx; i < zone_endIdx; i++) {
            int monthValue = Integer.parseInt(months.get(i).substring(5, 7));
            int yearValue = Integer.parseInt(months.get(i).substring(0, 4));
            if (yearValue == year && (monthValue == startMonth || monthValue == (startMonth + 1))) {
                pos.add(i);
            }
        }
    
        // Stage 2: Town filter
        List<String> towns = Files.readAllLines(Paths.get(DATA_DIR, "town.csv"));
        pos.removeIf(idx -> !towns.get(idx).equalsIgnoreCase(town));
    
        // Stage 3: Area filter
        List<String> areas = Files.readAllLines(Paths.get(DATA_DIR, "floor_area_sqm.csv"));
        pos.removeIf(idx -> Double.parseDouble(areas.get(idx)) < 80);
    
        // Final: Get filtered data
        List<String> prices = Files.readAllLines(Paths.get(DATA_DIR, "resale_price.csv"));
        List<String[]> filtered = new ArrayList<>();
        for (int i : pos) {
            filtered.add(new String[]{prices.get(i), areas.get(i)});
        }
    
        return filtered;
    }
    
    // As per lecture, the shared scan performs a single-stage filter on the data based on the year, month, town, and area
    private static List<String[]> sharedScan(int year, int startMonth, String town, int zone_startIdx, int zone_endIdx) throws IOException {
        List<String> months = Files.readAllLines(Paths.get(DATA_DIR, "month.csv"));
        List<String> towns = Files.readAllLines(Paths.get(DATA_DIR, "town.csv"));
        List<String> areas = Files.readAllLines(Paths.get(DATA_DIR, "floor_area_sqm.csv"));
        List<String> prices = Files.readAllLines(Paths.get(DATA_DIR, "resale_price.csv"));
        
        List<String[]> filtered = new ArrayList<>();
    
        if(zone_endIdx == Integer.MAX_VALUE){ // If zone_endIdx is not set (for non-zone index queries), set it to the size of the months list to perform a full column scan
            zone_endIdx = months.size();
        }
        else{
            zone_endIdx = zone_endIdx + 1;
        }
        for (int i = zone_startIdx; i < zone_endIdx; i++) {
            int monthValue = Integer.parseInt(months.get(i).substring(5, 7));
            int yearValue = Integer.parseInt(months.get(i).substring(0, 4));
            if ((yearValue == year) && (monthValue == startMonth || monthValue == (startMonth + 1)) && towns.get(i).equalsIgnoreCase(town) && Double.parseDouble(areas.get(i)) >= 80) {
                filtered.add(new String[]{prices.get(i), areas.get(i)});
            }
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
        double variance = values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).sum() / values.size();
        return Math.sqrt(variance);
    }
    
        
    private static void writeStatisticsToCSV(Map<String, Double> stats, int year, int startMonth, String town, String outputFile) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("Year,Month,Town,Category,Value\n");
            for (var entry : stats.entrySet()) {
            writer.write(year + "," + startMonth + "," + town + "," + entry.getKey() + "," + String.format("%.2f", entry.getValue()) + "\n");
            }
        }
        System.out.println("Results saved to " + outputFile);
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
        long startTime, endTime;

        String csvPath = "ResalePricesSingapore.csv";

        // One-time preprocessing step
        sortCSVByMonth(csvPath, OUTPUTCSV);
        splitCSV(OUTPUTCSV); 

        try (Scanner userInput = new Scanner(System.in)) {
            System.out.println("Enter Matriculation No.");
            String matricNo = userInput.nextLine();  
            System.out.println("Matriculation No: " + matricNo); 

            // Extract year, month, and town from matriculation number
            QueryParams params = matricExtraction(matricNo);
            int year = params.year();
            int startMonth = params.month();
            String town = params.town();

            // Normal Query
            System.out.println("Running Normal Query...");
            startTime = System.currentTimeMillis();
            normalQuery(year, startMonth, town);
            endTime = System.currentTimeMillis();
            System.out.println("Normal Query Time: " + (endTime - startTime) + " ms");

            // Shared Scan Query
            System.out.println("Running Shared Scan Query...");
            startTime = System.currentTimeMillis();
            ssQuery(year, startMonth, town);
            endTime = System.currentTimeMillis();
            System.out.println("Shared Scan Query Time: " + (endTime - startTime) + " ms");

            // Generate zones for zone mapping
            Map<String, Map<String, Integer>> zones = generateZones();
            // Zone Mapping Query
            System.out.println("Running Zone Mapping Query...");
            startTime = System.currentTimeMillis();
            zmQuery(year, startMonth, town, zones);
            endTime = System.currentTimeMillis();
            System.out.println("Zone Mapping Query Time: " + (endTime - startTime) + " ms");
            
            // Zone Mapping + Shared Scan Query
            System.out.println("Running Zone Mapping + Shared Scan Query...");
            startTime = System.currentTimeMillis();
            zmssQuery(year, startMonth, town, zones);
            endTime = System.currentTimeMillis();
            System.out.println("Zone Mapping + Shared Scan Query Time: " + (endTime - startTime) + " ms");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
        
    }
}
