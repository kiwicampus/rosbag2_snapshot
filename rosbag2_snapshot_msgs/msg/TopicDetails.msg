# Per-topic overrides for one capture. -1 / "" = keep the topic's configured value.
string name     # Buffered topic name; entries are matched by name
string type     # Ignored on a goal

float32 throttle_period -1.0          # Minimum seconds between written messages
int8 h264_throttle_skip -1            # (int-bool) Ignore throttle_period while the topic is written as H264

int8 override_old_timestamps -1       # (int-bool) Write messages received before start_time at start_time
int32 queue_depth -1                  # Write at most the newest N messages. 0 = no cap
int32 old_messages_to_keep -1         # Also write up to N messages from before start_time. 0 = none

int8 use_compression -1     # (int-bool) Compress this image topic. 1 with no format here or in the topic's config = jpg
string format ""            # jpg, jpeg, png or h264; any other value disables compression
int32 jpg_quality -1        # 0-100, read only with format jpg/jpeg
int32 png_compression -1    # 0-9, read only with format png

int8 include_post_trigger -1  # (int-bool) In a forward capture, include arrivals after the trigger
