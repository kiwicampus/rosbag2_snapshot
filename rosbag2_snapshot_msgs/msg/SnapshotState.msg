bool recording                           # Buffering is enabled (not paused)
uint32 active_capture_count              # Captures in progress
uint32 buffered_topic_count              # Number of buffered topics
# Outcome of the most recently finished capture.
bool has_last_capture                    # False until a capture has finished
bool last_capture_success
string last_capture_message              # Saved path on success, else the reason
builtin_interfaces/Time last_capture_stamp
string[] buffered_topics                 # Names of the buffered topics
float32 buffered_window_s                # Longest oldest-to-newest span of any buffer, seconds
