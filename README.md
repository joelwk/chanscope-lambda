### Configuration File Documentation

This documentation provides a detailed explanation of each section and parameter in the configuration file. This configuration file is used for processing and managing data collected from 4chan. Each section corresponds to a specific aspect of the data collection, processing, and storage pipeline.

---

### **[general]**
This section contains general configuration settings for the application.

- **`url`**: The base URL used for accessing the forum data.  
  Example: `https://a.4cdn.org/`

- **`time_format`**: The time format used when parsing or formatting time values.  
  Example: `%%H:%%M:%%S` corresponds to `Hours:Minutes:Seconds`.

- **`date_format`**: The date format used when parsing or formatting date values.  
  Example: `%%Y:%%m:%%d` corresponds to `Year-Month-Day`.

- **`path_temp`**: The directory path for storing temporary files.  
  Default: `tmp`

- **`path_Ftype`**: File type for output files.  
  Default: `csv`

- **`lookback_days`**: The number of days to look back when processing data.  
  Default: `30`

- **`process_data`**: A boolean flag to enable or disable data processing.  
  Default: `True`

- **`padding`**: A boolean flag to indicate whether to pad data for consistent formatting.  
  Default: `False`

- **`contraction_mapping`**: A boolean flag to enable contraction expansion (e.g., "can't" -> "cannot").  
  Default: `False`

- **`non_alpha_numeric`**: A boolean flag to remove non-alphanumeric characters during text cleaning.  
  Default: `False`

---

### **[thread_info]**
This section contains configuration details related to thread-level information.

- **`threads_key`**: The key name for the list of threads in the data.

- **`thread_number_key`**: The key for the unique thread identifier in the source data.  
  Default: `no`

- **`thread_cmt_number_key`**: The key for the total number of comments in the thread.  
  Default: `posts`

- **`matches`**: The column name to store matched key phrases.  
  Default: `matches`

- **`text_clean`**: The column name for storing cleaned text.  
  Default: `text_clean`

- **`p_com`**: The column name for storing the original posted comment.  
  Default: `posted_comment`

- **`collected_dt_key`**: The column name for the datetime the data was collected.  
  Default: `collected_date_time`

- **`posted_dt_key`**: The column name for the datetime the comment was posted.  
  Default: `posted_date_time`

- **`date_key`**: The column name for the date.  
  Default: `date`

- **`now_key`**: The column name for the current timestamp.  
  Default: `now`

- **`time_key`**: The column name for the time.  
  Default: `time`

- **`omit_ids`**: A list of thread IDs to omit from processing.  
  Example: `4884770, 21374000, 26847096, ...`

- **`boards`**: A comma-separated list of boards to process. The below are boards that are currently supported, add new boards as needed.  
  Example: `pol,biz,diy,his,sci`

---

### **[renamed]**
This section maps original column names to new standardized column names.

- **`no`**: Maps to `thread_id`.  
- **`sub`**: Maps to `thread_header`.  
- **`com`**: Maps to `posted_comment`.  

---

### **[board_specific]**
This section defines the specific columns to retain for each board during processing.

- **`biz_keys`**: Columns for the `biz` board.  
  Example: `thread_id, thread_header, posted_date_time, collected_date_time, ...`

- **`pol_keys`**: Columns for the `pol` board.  
  Example: `thread_id, thread_header, posted_date_time, collected_date_time, ...`

- **`diy_keys`**: Columns for the `diy` board.  
  Example: `thread_id, thread_header, posted_date_time, collected_date_time, ...`

- **`sci_keys`**: Columns for the `sci` board.  
  Example: `thread_id, thread_header, posted_date_time, collected_date_time, ...`

- **`his_keys`**: Columns for the `his` board.  
  Example: `thread_id, thread_header, posted_date_time, collected_date_time, ...`

---

### **[s3]**
This section contains configuration for AWS S3 bucket storage. All of this can be customized based on your AWS account and data structure.

- **`bucket`**: The name of the S3 bucket where data is stored.  
  Default: `chanscope-data`

- **`data_prefix`**: The prefix for data files in the S3 bucket.  
  Default: `data`

- **`raw_prefix`**: The prefix for raw files in the S3 bucket.  
  Default: `raw`

- **`processed_prefix`**: The prefix for processed files in the S3 bucket.  
  Default: `processed`

- **`models_prefix`**: The prefix for models stored in the S3 bucket.  
  Default: `models`

- **`padding_data`**: The padding suffix for data files.  
  Default: `_data_`

- **`padding_gather`**: The padding suffix for raw files.  
  Default: `_raw_`

- **`padding_processed`**: The padding suffix for processed files.  
  Default: `_processed_`

---

### **[s3_refresh_destinations]**
This section specifies the configuration for refreshing S3 data. Similar to the above section, this can be customized based on your AWS account and data structure.

- **`source_bucket`**: The source bucket for S3 data refresh.  
  Default: `chanscope-data`

- **`source_prefix`**: The prefix for the source data in the S3 bucket.  
  Default: `data`

- **`roling_bucket`**: The rolling bucket name for incremental updates.  
  Default: `rolling-data`

- **`daily_bucket_rolling`**: The daily rolling bucket name for market-stratified updates.  
  Default: `market-stratified`

- **`lookback_days`**: Number of days to look back for processing rolling data.  
  Default: `30`

- **`daily_lookback_days`**: Number of days to look back for processing daily data.  
  Default: `1`

---

### Notes:
1. **Key Phrases JSON**: Ensure that the `key_phrases.json` file exists and contains valid categories and phrases. Add key phrases as needed.
2. **S3 Buckets**: Update S3 bucket names and prefixes based on your AWS account and data structure.
3. **Custom Boards**: Add new boards in the `boards` list under `[thread_info]` and specify their keys under `[board_specific]`.
4. **Error Handling**: Validate column existence (e.g., `thread_id`, `posted_comment`) before processing to avoid runtime errors.

This configuration file is structured to handle large-scale forum data processing efficiently and flexibly. Update values as per your project's requirements.