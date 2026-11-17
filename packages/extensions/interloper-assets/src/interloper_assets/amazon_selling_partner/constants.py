REPORT_API_VERSION = "2021-06-30"
DATAKIOSK_API_VERSION = "2023-11-15"
DATAKIOSK_SCHEMA = "analytics_vendorAnalytics_2024_09_30"

# Seconds between two polls of a job, and each job's deadline: report jobs and Data Kiosk queries
# queue server-side, vendor reports for hours.
POLL_INTERVAL = 60.0
REPORT_TIMEOUT = 7200.0
QUERY_TIMEOUT = 3600.0
