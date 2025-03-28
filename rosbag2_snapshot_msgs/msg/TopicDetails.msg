string name     # The fully-qualified topic name
string type     # The fully-qualified message type

float32 throttle_period -1.0          # The period of the messages to be saved
int8 h264_throttle_skip -1          # (int-bool) If true, the throttle_period is ignored and all messages are saved

int8 override_old_timestamps -1      # (int-bool) This will override timestamps of old messages, placing as if they were at the beginning of the rosbag
int32 queue_depth -1                 # The number of messages to save in the rosbag
int32 old_messages_to_keep -1        # The number of messages to keep before the time window

int8 use_compression -1     # (int-bool) If true, the topic will be compressed
string format ""            # The format to use for compression. Choices: jpg, png
int32 jpg_quality -1        # The quality of the jpg compression. 0-100
int32 png_compression -1    # The quality of the png compression. 0-9
