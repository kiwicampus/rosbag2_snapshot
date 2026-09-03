# rosbag2_snapshot

Solution for [this rosbag2 issue](https://github.com/ros2/rosbag2/issues/663) which acts similarly to [`rosbag_snapshot`](https://github.com/ros/rosbag_snapshot). It is added as a new package here rather than patching `rosbag2` because that's how they did it in ROS 1 ;).

It subscribes to topics and maintains a buffer of recent messages like a dash cam. This is useful in live testing where unexpected events can occur which would be useful to have data on but the opportunity is missed if `rosbag record` was not running (disk space limits make always running `rosbag record` impracticable). Instead, users may run snapshot in the background and save data from the recent past to disk as needed.


## Usage

`rosbag2_snapshot` can be configured through ROS params for more granular control. The `snapshotter` command runs the buffer server; requests to write data to disk or to pause/resume buffering are made via the `trigger_snapshot` action and `enable_snapshot` service it exposes (see "Client" below) rather than a separate client binary.

### Server

```
$ ros2 run rosbag2_snapshot snapshotter
```

Buffer recent messages until triggered to write or trigger an already running instance.

### Example param file
```yaml
/**:
  ros__parameters:
    default_duration_limit: 10.0           # [Optional, default=-1] Maximum time difference between newest and oldest message in seconds
    default_memory_limit: 64.0             # [Optional, default=-1] Maximum memory used by messages in each topic's buffer, in MB
    interval_single_msg_types:             # [Optional] Message types narrowed to a single message by
      - "sensor_msgs/msg/NavSatFix"        # interval_mode_single_msg (see TriggerSnapshot), in addition to
                                            # CameraInfo, ImageMarker and compressed Image, which always get
                                            # this treatment. Only types that carry a real std_msgs/Header
                                            # with a builtin_interfaces/Time stamp qualify; others are
                                            # ignored with a warning.
    topics: ["/topic1", "/topic2"]         # [Optional] List of topics to buffer. If empty, buffer all topics.
    topic_details:
      /topic1:
        type: "sensor_msgs/msg/NavSatFix"  # [Required if topic is specified] Topic type
      /topic2:
        type: "sensor_msgs/msg/Odometry"
        duration: 15.0                     # [Optional] Override duration limit, inherit memory limit
      /topic3:
        type: "sensor_msgs/msg/Image"
        duration: 2.0                      # [Optional] Override both limits
        memory: -1                         # Negative value means no limit
    capture_profiles_dir: "/path/to/profiles"  # [Optional] See "Capture profiles" below
```

### Capture profiles

`capture_profiles_dir` points at a directory of `<name>.yaml` files, each one a named,
selectable topic set. This is independent of, and can be used alongside, the `topics`/
`topic_details` list above — a deployment can use either, both, or neither. It doesn't
replace `topics`/`topic_details`, and using it requires no change to that list.

```yaml
# capture_profiles/sensors.yaml
topics:
  - name: /imu
    max_rate_hz: 10.0        # [Optional] Downsample to at most one message per 1/max_rate_hz
                              # when this profile is selected. Omit to keep every message.
  - name: /odom
  - name: /camera/image_raw
    type: "sensor_msgs/msg/Image"  # [Optional] Explicit type/qos, same as topic_details above.
    qos: "SENSOR_DATA"             # If omitted, both are resolved from the ROS graph at
                                    # subscribe time (type from get_topic_names_and_types(),
                                    # QoS adapted to what the topic's publishers actually offer,
                                    # the way `ros2 bag record` does) -- retried on a timer if
                                    # the publisher hasn't appeared yet.
```

A profile can also nest other profiles via `include`, which is how you get "multiple profiles at
once" out of a single `TriggerSnapshot.profile` selection instead of adding request-side
complexity for it:

```yaml
# capture_profiles/incident.yaml
include: [sensors, video]  # a single name also works: include: sensors
topics:                    # [Optional] needs no topics of its own if it only combines others
  - name: /odom
    max_rate_hz: 5.0       # own entry overrides the inherited one (sensors' /odom) by name
  - name: /extra_topic
```

Only `topics` are inherited. Included profiles are merged in list order (a later one overrides
an earlier one on a shared topic name), and this profile's own `topics` always win over anything
inherited, regardless of include order. A profile that includes something unknown, or that ends
up with no topics once its includes are resolved, or that's part of an include cycle, is dropped
(reported as a warning at startup) -- the rest of the directory still loads.

Every topic named by any profile file is buffered continuously from startup, same as the
`topics` list -- there is no "subscribe only while this profile is active" mode. A late
subscription would miss everything published before it connects for any VOLATILE topic
(the default for most sensor topics), defeating the point of buffering ahead of a trigger.

If two separate (non-including) profile files both name the same topic, the one whose
profile name sorts first alphabetically wins that topic's type/qos/max_rate_hz for the
shared subscription -- profiles are otherwise independent, so there's no explicit priority
list to consult.

Select a profile in a `TriggerSnapshot` request via the new `profile` field (`""` = today's
behavior: `topics` on the request, or the full buffer, exactly as before). A named profile's
`max_rate_hz` always applies to that write, regardless of the request's `throttle_msgs` flag.
An unknown profile name is rejected at goal-acceptance time.

Note: `rosbag_preset_profile` (above) is an unrelated, differently-named existing setting --
it's the rosbag2 storage compression preset (e.g. `zstd_small`), not a capture profile.

### Concurrent captures & atomic writes

Multiple `TriggerSnapshot` goals for *different* filenames may be in flight at the same time --
this is a real, relied-upon usage pattern (e.g. `data_server_cpp` can schedule more than one
capture at once) and is not limited. The only rejection this package makes on that basis is a
second goal for a filename that's *already* being written, since two captures opening the same
output file concurrently would corrupt each other's output; it's rejected at goal-acceptance
time with a clear message, and freed up again as soon as the first capture finishes.

Every capture is written to a staging path in the same directory as the requested filename
(`<filename>.tmp`) and atomically renamed into place only once the write succeeds -- a crash or
`kill -9` mid-write leaves only the `.tmp` file behind, never a corrupt file at the real path. A
leftover `.tmp` from a previous crash is reclaimed the next time that exact filename is
requested again (there's no startup sweep, since this package has no "storage root" concept to
bound one to).

### Status & capture-event topics

Two additional topics report on the node's own state, independent of the per-request
action feedback/result:

- **`snapshot_state`** (`rosbag2_snapshot_msgs/msg/SnapshotState`, transient-local, so a late
  subscriber gets the current value immediately): `recording`, `active_capture_count`,
  `buffered_topic_count`, and the outcome of the most recently *finished* capture of any
  filename (`has_last_capture`, `last_capture_success`, `last_capture_message`,
  `last_capture_stamp`). Published on every state change (goal accepted/rejected, capture
  finalized, recording paused/resumed, a profile topic newly buffered).
- **`snapshot_capture_event`** (`rosbag2_snapshot_msgs/msg/SnapshotCaptureEvent`): one message
  per completed capture -- `filename`, `profile`, `success`, `message`, `topics_written`,
  `duration`, `stamp` -- so a consumer that needs to know about a *specific* capture (rather
  than the node's rolled-up last-capture summary above) doesn't have to race the state topic.

### Client

There is no separate client binary -- `trigger_snapshot` is a ROS 2 action (not a service),
served directly by the `snapshotter` node under its own namespace.

###### Trigger a snapshot of all buffered topics

```
$ ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: ''}"
```

###### Trigger a snapshot of selected topics via a named capture profile

```
$ ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '', profile: 'sensors'}"
```

###### Pause/resume buffering manually (this one really is a service)

```
$ ros2 service call /enable_snapshot std_srvs/srv/SetBool "{data: false}"  # false = pause, true = resume
requester: making request: std_srvs.srv.SetBool_Request(data=False)

response:
std_srvs.srv.SetBool_Response(success=True, message='')
```
