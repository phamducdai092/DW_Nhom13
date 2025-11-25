package staging_data;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.time.LocalDateTime; // Đã thêm: Xử lý đối tượng ngày giờ
import java.time.format.DateTimeFormatter; // Đã thêm: Định dạng ngày giờ

public class StagingDataMain {

    // Cấu hình kết nối DB Control
    private static final String DB_CONTROL_URL = "jdbc:mysql://localhost:3306/Control?useUnicode=true&characterEncoding=UTF-8";
    // Cấu hình kết nối DB Staging
    private static final String DB_STAGING_URL = "jdbc:mysql://localhost:3306/Staging?useUnicode=true&characterEncoding=UTF-8";
    private static final String USER = "root";
    private static final String PASS = "root";
    private static final String JOB_NAME = "StagingData";


    public static void main(String[] args) {
        Connection connControl = null;
        Connection connStaging = null;

        try {
            // 1. Kết nối DB Control & Staging
            System.out.println("--> Đang kết nối DB Control và Staging...");
            connControl = DriverManager.getConnection(DB_CONTROL_URL, USER, PASS);
            connStaging = DriverManager.getConnection(DB_STAGING_URL, USER, PASS);

            // 2. Check & Lock Job
            if (isJobRunning(connControl, JOB_NAME)) {
                log(connControl, "WARN", "Job " + JOB_NAME + " đang chạy, bỏ qua lần này.", JOB_NAME);
                return;
            }
            lockJob(connControl, JOB_NAME, true); // Set is_running = 1

            // 3. Load danh sách các file CSV có status = 'NEW'
            List<FileConfig> newFiles = getNewFileConfigs(connControl);

            if (newFiles.isEmpty()) {
                log(connControl, "INFO", "Không tìm thấy file CSV mới (status='NEW').", JOB_NAME);
                return;
            }

            int totalRecordsLoaded = 0;
            for (FileConfig fileConfig : newFiles) {
                System.out.println("--- Đang xử lý file: " + fileConfig.filePath + " (ID: " + fileConfig.id + ") ---");

                // 4. Update status thành PROCESSING
                updateConfigStatus(connControl, fileConfig.id, "PROCESSING");
                log(connControl, "INFO", "Bắt đầu xử lý file.", JOB_NAME);

                try {
                    // 5. Nạp dữ liệu từ CSV vào DB Staging
                    int loadedCount = loadCsvToStaging(connStaging, fileConfig);
                    totalRecordsLoaded += loadedCount;

                    // 6. Cập nhật status thành DONE
                    updateConfigStatus(connControl, fileConfig.id, "DONE");
                    log(connControl, "INFO", "Nạp thành công " + loadedCount + " dòng từ file: " + fileConfig.filePath, JOB_NAME);

                } catch (Exception e) {
                    // 8. Log lỗi và update status thành ERROR
                    e.printStackTrace();
                    log(connControl, "ERROR", "Lỗi xử lý file " + fileConfig.filePath + ": " + e.getMessage(), JOB_NAME);
                    updateConfigStatus(connControl, fileConfig.id, "ERROR");
                }
            }

            if(totalRecordsLoaded > 0) {
                log(connControl, "INFO", "Hoàn thành Job. Tổng cộng " + totalRecordsLoaded + " dòng đã được nạp vào Staging.", JOB_NAME);
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (connControl != null) log(connControl, "ERROR", "Crash: " + e.getMessage(), JOB_NAME);
            } catch (SQLException ex) { /* Ignore */ }
        } finally {
            // 9. Mở khóa Job (Luôn chạy)
            if (connControl != null) {
                lockJob(connControl, JOB_NAME, false); // Set is_running = 0
            }
            try {
                if (connStaging != null) connStaging.close();
                if (connControl != null) connControl.close();
            } catch (SQLException e) { /* Ignore */ }
            System.out.println("--> Kết thúc Job.");
        }
    }

    // --- CÁC HÀM HỖ TRỢ (HELPER METHODS) ---

    /**
     * Class nội bộ để chứa thông tin cấu hình file từ bảng configs.
     */
    private static class FileConfig {
        int id;
        String filePath;
        int configId;

        public FileConfig(int id, String filePath) {
            this.id = id;
            this.filePath = filePath;
            this.configId = id;
        }
    }

    /**
     * Truy vấn DB Control để lấy danh sách các file CSV có trạng thái 'NEW'.
     * @param conn Kết nối tới DB Control.
     * @return Danh sách các đối tượng FileConfig cần xử lý.
     * @throws SQLException Nếu có lỗi truy vấn DB.
     */
    private static List<FileConfig> getNewFileConfigs(Connection conn) throws SQLException {
        List<FileConfig> files = new ArrayList<>();
        String sql = "SELECT ID, file_path FROM configs WHERE file_status = 'NEW'";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                files.add(new FileConfig(rs.getInt("ID"), rs.getString("file_path")));
            }
        }
        return files;
    }

    /**
     * Đọc từng dòng từ file CSV, chuyển đổi định dạng ngày tháng và nạp dữ liệu vào bảng gold_prices trong DB Staging.
     * Sử dụng giao tác (Transaction) và Batch Update để tối ưu hiệu suất.
     * @param connStaging Kết nối tới DB Staging.
     * @param config Thông tin cấu hình file.
     * @return Số lượng dòng đã được nạp thành công.
     * @throws Exception Nếu file không tồn tại, hoặc có lỗi I/O, SQL.
     */
    private static int loadCsvToStaging(Connection connStaging, FileConfig config) throws Exception {
        int count = 0;
        File file = new File(config.filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("File không tồn tại: " + config.filePath);
        }

        String insertSql = "INSERT INTO gold_prices (config_id, buying, purchasing, Region, Update_time) VALUES (?, ?, ?, ?, ?)";
        connStaging.setAutoCommit(false); // Bắt đầu Transaction
        try (
                // Sử dụng InputStreamReader với StandardCharsets.UTF_8 để đọc đúng BOM/UTF-8
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
                PreparedStatement pstmt = connStaging.prepareStatement(insertSql)
        ) {
            // Định nghĩa định dạng đầu vào và đầu ra cho Datetime
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            String line;
            while ((line = reader.readLine()) != null) {
                // Xóa BOM (\ufeff) nếu có ở đầu file (thường do Excel ghi ra)
                if (count == 0 && line.length() > 0 && line.charAt(0) == '\ufeff') {
                    line = line.substring(1);
                }

                String[] parts = line.split(",");
                // Định dạng CSV: "tensp",giamua,giaban,"zone","updateTime" (5 cột)
                if (parts.length == 5) {
                    String productName = parts[0].replaceAll("^\"|\"$", "");
                    String buyingPrice = parts[1];
                    String purchasingPrice = parts[2];
                    String region = parts[3].replaceAll("^\"|\"$", "");

                    // Lấy giá trị thô từ CSV
                    String updateTimeRaw = parts[4].replaceAll("^\"|\"$", "");

                    // Parse từ format DD/MM/YYYY HH:MM:SS sang YYYY-MM-DD HH:MM:SS (Định dạng SQL)
                    LocalDateTime dateTime = LocalDateTime.parse(updateTimeRaw, inputFormatter);
                    String updateTimeFormatted = dateTime.format(outputFormatter);

                    // Nạp vào bảng GoldPrices (5 tham số)
                    pstmt.setInt(1, config.configId); // Index 1: config_id
                    pstmt.setString(2, buyingPrice); // Index 2: buying
                    pstmt.setString(3, purchasingPrice); // Index 3: purchasing
                    pstmt.setString(4, region); // Index 4: Region
                    pstmt.setString(5, updateTimeFormatted); // Index 5: Update_time (Đã format)

                    pstmt.addBatch(); // Thêm vào lô xử lý
                    count++;
                }
            }

            pstmt.executeBatch(); // Thực thi lô xử lý
            connStaging.commit(); // Commit Transaction
        } catch (Exception e) {
            connStaging.rollback(); // Rollback nếu có lỗi
            throw e;
        } finally {
            connStaging.setAutoCommit(true);
        }
        return count;
    }

    /**
     * Cập nhật trạng thái xử lý của file (PROCESSING, DONE, ERROR) trong bảng configs.
     * @param conn Kết nối tới DB Control.
     * @param id ID của file trong bảng configs.
     * @param status Trạng thái mới.
     * @throws SQLException Nếu có lỗi truy vấn DB.
     */
    private static void updateConfigStatus(Connection conn, int id, String status) throws SQLException {
        String sql = "UPDATE configs SET file_status = ?, processed_at = NOW() WHERE ID = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status);
            pstmt.setInt(2, id);
            pstmt.executeUpdate();
        }
    }

    /**
     * Ghi thông tin log (INFO, ERROR, WARN) vào bảng log trong DB Control.
     * @param conn Kết nối tới DB Control.
     * @param level Mức độ log (INFO, ERROR, WARN).
     * @param msg Nội dung thông báo.
     * @param jobName Tên Job đang chạy.
     * @throws SQLException Nếu có lỗi truy vấn DB.
     */
    private static void log(Connection conn, String level, String msg, String jobName) throws SQLException {
        String sql = "INSERT INTO log (job_name, log_level, message) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, jobName);
            pstmt.setString(2, level);
            pstmt.setString(3, msg);
            pstmt.executeUpdate();
            System.out.println("[" + level + "] " + msg);
        }
    }

    /**
     * Kiểm tra xem Job hiện tại có đang được đánh dấu là đang chạy (is_running = 1) trong bảng job_status hay không.
     * @param conn Kết nối tới DB Control.
     * @param jobName Tên Job cần kiểm tra.
     * @return true nếu Job đang chạy, ngược lại là false.
     * @throws SQLException Nếu có lỗi truy vấn DB.
     */
    private static boolean isJobRunning(Connection conn, String jobName) throws SQLException {
        String sql = "SELECT is_running FROM job_status WHERE job_name = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);) {
            pstmt.setString(1, jobName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) return rs.getBoolean("is_running");
            }
        }
        return false;
    }

    /**
     * Đặt hoặc loại bỏ khóa Job (is_running = true/false) trong bảng job_status.
     * @param conn Kết nối tới DB Control.
     * @param jobName Tên Job.
     * @param is_running Trạng thái khóa (true để khóa, false để mở khóa).
     */
    private static void lockJob(Connection conn, String jobName, boolean is_running) {
        try (PreparedStatement pstmt = conn.prepareStatement(
                "UPDATE job_status SET is_running = ?, last_run = NOW() WHERE job_name = ?")) {
            pstmt.setBoolean(1, is_running);
            pstmt.setString(2, jobName);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}