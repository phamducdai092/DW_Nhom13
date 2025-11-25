package crawl_data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Main {

    // Cấu hình kết nối DB Control
    private static final String DB_URL = "jdbc:mysql://localhost:3306/Control?useUnicode=true&characterEncoding=UTF-8";
    private static final String USER = "root";
    private static final String PASS = "root";

    // Format ngày tháng của PNJ (dd/MM/yyyy HH:mm:ss)
    private static final DateTimeFormatter PNJ_DATE_FMT = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
    // Format ngày tháng chuẩn SQL (yyyy-MM-dd HH:mm:ss)
    private static final DateTimeFormatter SQL_DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        Connection conn = null;
        try {
            // 1. Kết nối DB Control
            System.out.println("--> Đang kết nối DB Control...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // 2. Check
            // 3. Lock Job
            if (isJobRunning(conn)) {
                log(conn, "WARN", "Job CrawlData đang chạy, bỏ qua lần này.");
                return;
            }
            lockJob(conn, true); // Set is_running = 1

            // 4. Load Config từ bảng source_config
            String apiUrlTemplate = getConfig(conn, "API_URL"); // "...?zone=%s"
            String[] regions = getConfig(conn, "REGION_ZONE").split(","); // "00,20"
            String lastUpdateTimeStr = getConfig(conn, "LAST_UPDATE_TIME");

            // Parse thời gian cũ từ DB
            LocalDateTime lastTime = LocalDateTime.parse(lastUpdateTimeStr, SQL_DATE_FMT);
            System.out.println("--> Mốc thời gian cũ trong DB: " + lastTime);

            // Biến để lưu thời gian mới nhất tìm được
            LocalDateTime maxUpdateDate = lastTime;
            boolean hasNewData = false;

            // 5. Vòng lặp từng khu vực
            for (String zone : regions) {
                System.out.println("--- Đang xử lý Zone: " + zone + " ---");

                // 6. Call API
                String url = String.format(apiUrlTemplate, zone);
                String jsonResponse = callApi(url);

                // Parse JSON
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(jsonResponse);
                String updateDateStr = root.get("updateDate").asText(); // "04/11/2025 08:46:46"
                JsonNode dataList = root.get("data");

                LocalDateTime apiTime = LocalDateTime.parse(updateDateStr, PNJ_DATE_FMT);

                // 7. Kiểm tra dữ liệu mới
                if (apiTime.isAfter(lastTime)) {
                    System.out.println("--> Phát hiện dữ liệu MỚI (" + apiTime + ")");
                    hasNewData = true;
                    if (apiTime.isAfter(maxUpdateDate)) maxUpdateDate = apiTime;

                    // 8. Tạo file CSV & Insert Config 'NEW'
                    String fileName = "dw_" + zone + "_" + System.currentTimeMillis() + ".csv";
                    // Lưu ý: Folder này phải tồn tại trên máy
                    String fullPath = "D:\\Documents\\Data Warehouse\\" + fileName;

                    int configId = insertConfig(conn, fullPath, "NEW");

                    // 9. Ghi file CSV
                    try {
                        writeCsvFile(dataList, fullPath, zone, updateDateStr);
                        // 10. Log INFO
                        log(conn, "INFO", "Ghi file thành công: " + fileName);
                    } catch (Exception e) {
                        // Nhánh Failed của bước 9
                        // 11. Log ERROR
                        e.printStackTrace();
                        log(conn, "ERROR", "Lỗi ghi file CSV: " + e.getMessage());
                        updateConfigStatus(conn, configId, "ERROR");
                    }

                } else {
                    // Nhánh False của bước 7
                    // 12. Log WARN
                    log(conn, "WARN", "Dữ liệu vùng " + zone + " cũ (" + apiTime + "), bỏ qua.");
                }
            }

            // 13. Cập nhật thời gian mới vào DB (Sau khi hết vòng lặp)
            if (hasNewData) {
                updateSourceConfig(conn, "LAST_UPDATE_TIME", maxUpdateDate.format(SQL_DATE_FMT));
                System.out.println("--> Đã cập nhật LAST_UPDATE_TIME lên: " + maxUpdateDate);
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (conn != null) log(conn, "ERROR", "Crash: " + e.getMessage());
            } catch (SQLException ex) { /* Ignore */ }
        } finally {
            // 14. Mở khóa Job (Luôn chạy)
            if (conn != null) {
                lockJob(conn, false); // Set is_running = 0
                updateLastRun(conn);
                try {
                    conn.close();
                } catch (SQLException e) {
                }
                System.out.println("--> Kết thúc Job.");
            }
        }
    }

    // --- CÁC HÀM HỖ TRỢ (HELPER METHODS) ---

    private static boolean isJobRunning(Connection conn) throws SQLException {
        String sql = "SELECT is_running FROM job_status WHERE job_name = 'CrawlData'";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) return rs.getBoolean("is_running");
        }
        return false;
    }

    private static void updateLastRun(Connection conn) {
        String sql = "UPDATE job_status SET last_run = ? WHERE Job_name = 'CrawlData'";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDate(1, java.sql.Date.valueOf(LocalDate.now()));
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void lockJob(Connection conn, boolean is_running) {
        try (PreparedStatement pstmt = conn.prepareStatement(
                "UPDATE job_status SET is_running = ? WHERE job_name = 'CrawlData'")) {
            pstmt.setBoolean(1, is_running);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String getConfig(Connection conn, String key) throws SQLException {
        String sql = "SELECT config_value FROM source_config WHERE config_key = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, key);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) return rs.getString("config_value");
            }
        }
        throw new SQLException("Config not found: " + key);
    }

    private static void updateSourceConfig(Connection conn, String key, String value) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(
                "UPDATE source_config SET config_value = ? WHERE config_key = ?")) {
            pstmt.setString(1, value);
            pstmt.setString(2, key);
            pstmt.executeUpdate();
        }
    }

    private static int insertConfig(Connection conn, String path, String status) throws SQLException {
        String sql = "INSERT INTO configs (file_path, file_status, create_at) VALUES (?, ?, NOW())";
        try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, path);
            pstmt.setString(2, status);
            pstmt.executeUpdate();
            try (ResultSet rs = pstmt.getGeneratedKeys()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return -1;
    }

    private static void updateConfigStatus(Connection conn, int id, String status) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("UPDATE configs SET file_status = ? WHERE ID = ?")) {
            pstmt.setString(1, status);
            pstmt.setInt(2, id);
            pstmt.executeUpdate();
        }
    }

    private static void log(Connection conn, String level, String msg) throws SQLException {
        String sql = "INSERT INTO log (job_name, log_level, message) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, "CrawlData");
            pstmt.setString(2, level);
            pstmt.setString(3, msg);
            pstmt.executeUpdate();
            System.out.println("[" + level + "] " + msg);
        }
    }

    private static String callApi(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private static void writeCsvFile(JsonNode dataList, String filePath, String region, String updateTime) throws IOException {
        File file = new File(filePath);
        if (file.getParentFile() != null) file.getParentFile().mkdirs();

        // SỬA ĐOẠN NÀY: Dùng FileOutputStream + OutputStreamWriter với UTF-8
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {

            // [MẸO CỦA GIÁO SƯ]: Thêm dòng này để Excel trên Windows tự nhận diện UTF-8 (BOM)
            // Nếu không có dòng này, mở bằng Excel sẽ vẫn bị lỗi font, dù code đúng.
            writer.write('\ufeff');

            // Phần ghi dữ liệu giữ nguyên
            for (JsonNode item : dataList) {
                String name = item.get("tensp").asText();
                String buy = item.get("giamua").asText();
                String sell = item.get("giaban").asText();

                String line = String.format("\"%s\",%s,%s,\"%s\",\"%s\"",
                        name, buy, sell, region, updateTime);
                writer.write(line);
                writer.newLine();
            }
        }
    }
}
