USE Control;

INSERT INTO source_config (config_key, config_value, description) VALUES
('API_URL', 'https://edge-api.pnj.io/ecom-frontend/v1/get-gold-price?zone=%s', 'URL of the gold prices data source. Replace %s with region code'),
('UPDATE_FREQUENCY', '60', 'Frequency (in minutes) to update gold prices data'),
('REGION_ZONE', '00,07,11,13,14,21', 'Comma-separated list of regions to fetch gold prices for, e.g., 00 for HCM, 07 for Can Tho, 11 for Ha Noi, 
13 for Da Nang, 14 for Tay Nguyen, 21 for Dong Nam Bo'),
('LAST_UPDATE_TIME', CURRENT_TIMESTAMP, 'Timestamp of the last successful data fetch from the source API');

INSERT INTO job_status (job_name, is_running, last_run) VALUES
('CrawlData', FALSE, NULL),
('Staging_Data', FALSE, NULL),
('LoadGoldPricesToDW', FALSE, NULL),
('LoadDWToDM', FALSE, NULL);

INSERT INTO log (job_name, log_level, message) VALUES
('System', 'INFO', 'Database initialized with seed data.');

INSERT INTO Dim_Region(region_key, region_name) VALUES 
(00, "Hồ Chí Minh"),
(07, "Cần Thơ"),
(11, "Hà Nội"),
(13, "Đà Nẵng"),
(14, "Tây Nguyên"),
(21, "Đông Nam Bộ");

