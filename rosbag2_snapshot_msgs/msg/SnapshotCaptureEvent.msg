# One message per finished capture.
string filename                  # Path actually written (.partial suffix when it ended early)
string profile                   # The goal's profile; empty when none
bool success                     # True only for a complete capture
string message                   # Saved path on success, else the reason
uint32 topics_written            # Topics the capture went through, skipped ones included
float32 duration                 # Seconds since the capture started
builtin_interfaces/Time stamp    # Publish time
# Every topic the bag declares and the messages written on it (parallel
# arrays); a declared topic with no message counts 0.
string[] content_topics
uint64[] content_message_counts
# Receipt time of the first and last message written; zero when none.
builtin_interfaces/Time first_message_stamp
builtin_interfaces/Time last_message_stamp
