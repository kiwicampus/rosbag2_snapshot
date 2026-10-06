string filename
string profile
bool success
string message
uint32 topics_written
float32 duration
builtin_interfaces/Time stamp
# Every topic the bag declares and the messages written on it (parallel
# arrays); a declared topic with no message counts 0.
string[] content_topics
uint64[] content_message_counts
# Receipt time of the first and last message written; zero when none.
builtin_interfaces/Time first_message_stamp
builtin_interfaces/Time last_message_stamp
