string name     # The fully-qualified topic name
string type     # The fully-qualified message type

int32 throttle_period -1          # The period of the messages to be saved
bool h264_throttle_skip false     # If true, the throttle_period is ignored and all messages are saved

bool override_old_timestamps false  # This will override timestamps of old messages, placing as if they were at the beginning of the rosbag
int32 queue_depth -1                 # The number of messages to save in the rosbag
int32 old_messages_to_keep -1        # The number of messages to keep before the time window

bool compression false   # If true, the topic will be compressed
string format ""         # The format to use for compression
int32 jpg_quality 0      # The quality of the jpg compression
int32 png_compression 0  # The quality of the png compression
