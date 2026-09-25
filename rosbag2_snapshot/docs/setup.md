# Setting up rosbag2_snapshot

The `snapshotter` node buffers topics in memory and writes them to an MCAP
bag when a `TriggerSnapshot` goal arrives. The interfaces are listed in the
[README](../../README.md#interfaces).

## Prerequisites

| Dependency | Notes |
|---|---|
| ROS 2 (`rclcpp`, `rclcpp_action`, `rclcpp_components`, `std_srvs`, `sensor_msgs`, `visualization_msgs`) | |
| `rosbag2_cpp`, `rosbag2_compression`, `rosbag2_compression_zstd`, `rosbag2_transport` | |
| rosbag2 MCAP storage plugin | Bags are always written with storage id `mcap` |
| `cv_bridge`, OpenCV | JPG/PNG image compression |
| `yaml-cpp` | |
| `foxglove_msgs` and FFmpeg (`libavcodec`, `libswresample`, `libswscale`, `libavutil`) | Optional. H264 is built only when both are found |

## Build

```bash
colcon build --symlink-install --packages-up-to rosbag2_snapshot
```

H264 support is detected at configure time: when `foxglove_msgs` or the
FFmpeg libraries are missing, CMake prints a warning and builds without H264.
FFmpeg is located with pkg-config; set the CMake variable `FFMPEG_PKGCONFIG`
to the directory holding its `.pc` files if they are outside the default
search path. `package.xml` lists `foxglove_msgs` as a dependency, so rosdep
installs it.

## Minimal example

```yaml
# my_params.yaml
/**:
  ros__parameters:
    default_duration_limit: 30.0
    default_memory_limit: 64.0
    topics: ["/odom", "/camera/image_raw"]
    topic_details:
      /odom:
        type: "nav_msgs/msg/Odometry"
      /camera/image_raw:
        type: "sensor_msgs/msg/Image"
        duration: 10.0
        compression:
          enabled: true
          format: "jpg"
          jpg_quality: 80
```

```bash
ros2 run rosbag2_snapshot snapshotter --ros-args --params-file my_params.yaml
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/snapshot.bag'}"
```

`param/single_topic.params.yaml` and `param/multiple_topics.params.yaml` are
further examples.

## Node parameters

| Parameter | Default | Meaning |
|---|---|---|
| `default_duration_limit` | `-1.0` | Per-topic buffer age limit, seconds. `-1` = no age limit (see [Time windows](#time-windows)) |
| `default_memory_limit` | `300.0` | Per-topic buffer size limit, MB (1 MB = 1,000,000 bytes). `-1` = no limit |
| `total_memory_limit` | `0.0` | Cap across all buffers combined, MB. `<= 0` = no shared cap |
| `max_post_duration_s` | `300.0` | Longest accepted `post_duration_s`. `<= 0` disables forward captures |
| `rosbag_preset_profile` | `"zstd_small"` | MCAP storage preset used when a goal leaves its own `rosbag_preset_profile` empty |
| `interval_single_msg_types` | `[]` | Extra message types narrowed to one message by `interval_mode_single_msg` |
| `capture_profiles_dir` | `""` | Directory of capture profiles. Empty = no profiles |
| `topics` | `[]` | Topics to buffer, each configured under `topic_details`. **Empty = buffer every topic in the graph** |
| `topic_details.<topic>.*` | | Per-topic settings, below |
| `h264.*` | | H264 encoder settings, below. H264 builds only |

Leaving `topics` empty buffers every topic, even when `capture_profiles_dir`
is set. To buffer only profile topics, list at least one topic in `topics`.

### Per-topic settings (`topic_details.<topic>`)

Only topics listed in `topics` read these keys.

| Key | Default | Meaning |
|---|---|---|
| `type` | required | Message type, e.g. `sensor_msgs/msg/Image` |
| `qos` | `DEFAULT` | `DEFAULT` (reliable, depth 5), `SENSOR_DATA` (best effort, depth 5) or `TRANSIENT_LOCAL` (depth 5). An unknown value logs an error and uses `DEFAULT` |
| `duration` | `default_duration_limit` | Buffer age limit, seconds. `-1` = no limit |
| `memory` | `default_memory_limit` | Buffer size limit, **bytes**. Negative = no limit |
| `throttle_period` | `-1.0` | Minimum seconds between written messages, applied when the goal sets `throttle_msgs` |
| `queue_depth` | `-1` | Write at most the newest N messages in range. `-1` = no cap. Ignored in interval mode |
| `old_messages_to_keep` | `-1` | Also write up to N messages from before `start_time` |
| `override_old_timestamps` | `false` | See [Timestamps](#timestamps) |
| `h264_throttle_skip` | `false` | Skip `throttle_period` while the topic is written as H264 |
| `compression.enabled` | `false` | Compress this topic when written. `sensor_msgs/msg/Image` topics only |
| `compression.format` | `jpg` | `jpg` (or `jpeg`) or `png`. Any other value disables compression |
| `compression.jpg_quality` | `95` | 0 to 100 |
| `compression.png_compression` | `3` | 0 to 9 |

A compressed topic is written as `sensor_msgs/msg/CompressedImage`.

### H264 encoder (`h264.*`)

One set for the whole node. Read only in an H264 build.

| Key | Default |
|---|---|
| `h264.encoding` | `"libx264"` |
| `h264.profile` | `""` |
| `h264.preset` | `""` |
| `h264.tune` | `""` |
| `h264.delay` | `""` |
| `h264.qmax` | `10` |
| `h264.bit_rate` | `8242880` |
| `h264.gop_size` | `15` |
| `h264.pixel_format` | `""` |

A topic written as H264 is stored as `foxglove_msgs/msg/CompressedVideo`.
H264 applies only to topics that are compressed (`compression.enabled`, or a
profile `compression` other than `none`); it is selected per goal with
`use_h264`, or per profile topic with `compression: h264`. Without an H264
build, those topics fall back to their JPG/PNG setting.

## Capture profiles

`capture_profiles_dir` holds one `<name>.yaml` file per profile; the file
stem is the profile name. Only `.yaml` files are read. A goal selects a
profile by name in `profile`.

```yaml
# sensors.yaml
topics:
  - name: /imu/data
    max_rate_hz: 10.0
  - name: /odom
  - name: /camera/image_raw
    type: sensor_msgs/msg/Image
    qos: SENSOR_DATA
    compression: jpg
    compression_quality: 80
    include_post_trigger: false
```

```yaml
# incident.yaml
include: [sensors, video]   # a single name also works
topics:
  - name: /odom
    max_rate_hz: 5.0        # replaces the /odom entry from sensors
```

### Profile topic keys

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Topic name |
| `type` | resolved from the graph | Message type |
| `qos` | adapted to the publishers' offered QoS | `DEFAULT`, `SENSOR_DATA` or `TRANSIENT_LOCAL` |
| `duration_s` | `default_duration_limit` | Buffer age limit, seconds. `> 0`, or `-1` for no limit |
| `memory_mb` | `default_memory_limit` | Buffer size limit, MB. `> 0` |
| `max_rate_hz` | `0` (every message) | Write at most one message per `1/max_rate_hz` seconds. `>= 0` |
| `include_post_trigger` | `true` | In a forward capture, `false` writes only what was buffered at the trigger |
| `compression` | topic's own setting | `jpg`, `png`, `h264` or `none`. Applies to image topics; other types are written uncompressed with a warning |
| `compression_quality` | `95` (jpg), `3` (png) | jpg 0 to 100, png 0 to 9. Requires `compression: jpg` or `png` |
| `override_old_timestamps` | topic's own setting | As in `topic_details` |
| `queue_depth` | topic's own setting | As in `topic_details`. `> 0` |
| `old_messages_to_keep` | topic's own setting | As in `topic_details`. `> 0` |
| `h264_throttle_skip` | topic's own setting | As in `topic_details` |

`type`, `qos`, `duration_s` and `memory_mb` set how the topic is buffered.
The other keys apply when the profile is selected.

### Profile rules

- Every profile topic is buffered from startup, whether or not a profile is
  selected. A topic whose type or QoS cannot be resolved yet (no publisher)
  is retried every second.
- A topic already listed in `topics` keeps its `topic_details` buffering.
  When several profiles name the same topic, the profile whose name sorts
  first sets its buffering (`type`, `qos`, `duration_s`, `memory_mb`).
- `include` accepts a name or a list. Only `topics` are inherited. Includes
  merge in list order, later entries replacing earlier ones by topic name,
  and the profile's own `topics` replace inherited ones.
- A profile is dropped, with a startup warning, when its file fails to parse
  or fails validation, it includes an unknown or dropped profile, it is part
  of an include cycle, or it ends up with no topics. The rest still load.
  For duplicate names, the first file found is kept.

## Triggering a capture

Send a `trigger_snapshot` goal. Every field is optional except `filename`.

### Goal fields

| Field | Default | Meaning |
|---|---|---|
| `filename` | required | Output path. Must end in `.bag` or `.mcap`, else the goal is rejected |
| `use_flat_output` | `false` | `false`: write a bag directory at `filename`. `true`: write a single `.mcap` file at `filename` |
| `profile` | `""` | Capture profile to write. Empty = use `topics`. An unknown name is rejected |
| `topics` | `[]` | `TopicDetails` entries; only `name` is required, and each other field set overrides that topic's setting for this capture. Empty = every buffered topic. Ignored when `profile` is set |
| `start_time` | `0` | Earliest message to write. `0` = oldest buffered |
| `stop_time` | `0` | Latest message to write. `0` = newest buffered |
| `post_duration_s` | `0.0` | `> 0` makes a [forward capture](#forward-captures) |
| `throttle_msgs` | `false` | Apply each topic's `throttle_period`. A profile's `max_rate_hz` applies regardless |
| `use_h264` | `false` | Write compressed image topics as H264 |
| `rosbag_preset_profile` | `""` | MCAP storage preset. Empty = the node parameter |
| `use_interval_mode` | `false` | Write `[msg_timestamp - interval_mode_tolerance, msg_timestamp + interval_mode_tolerance]` instead of `start_time`/`stop_time` |
| `msg_timestamp` | `0` | Interval center |
| `interval_mode_tolerance` | `0.0` | Interval half-width, seconds |
| `interval_mode_single_msg` | `false` | In interval mode, write one message per topic for `sensor_msgs/msg/CameraInfo`, `visualization_msgs/msg/ImageMarker`, compressed `sensor_msgs/msg/Image` topics and `interval_single_msg_types`: the one whose header stamp equals `msg_timestamp`, else the closest. Types without a `std_msgs/Header` keep the whole interval |

A requested topic that is not buffered is skipped with a warning. The goal is
also rejected when `post_duration_s` exceeds `max_post_duration_s` or forward
captures are disabled, or when a capture to the same `filename` is still in
progress. Captures to different filenames run concurrently.

### Result and feedback

| Field | Meaning |
|---|---|
| result `success` | `true` only for a complete capture |
| result `message` | Saved path on success, else the reason |
| feedback `progress` | Percent complete |
| feedback `duration` | Seconds since the capture started |
| feedback `message` | Current step |

`success` is the authoritative outcome. The action status is `SUCCEEDED`
for both complete and failed captures, `CANCELED` for a goal canceled while
writing, and `ABORTED` when the output cannot be opened or the capture
cannot start.

### Output files

| Outcome | `use_flat_output: false` | `use_flat_output: true` |
|---|---|---|
| Complete | Directory `<filename>/` with `<basename>_0.mcap` and `metadata.yaml` | File `<filename>` |
| Canceled or a topic failed to write | Same layout at `<filename>.partial` | File `<filename>.partial` |
| Writer failed to close | Left at the staging directory `<filename>.tmp` | Same |

Data is staged in `<filename>.tmp` and moved into place when the writer
closes. A leftover `<filename>.tmp` is overwritten by the next capture to the
same `filename`. `snapshot_capture_event.filename` gives the path actually
written.

### Examples

```bash
# Every buffered topic
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/all.bag'}"

# A named profile, as a single file
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/sensors.mcap', profile: 'sensors', use_flat_output: true}"

# Forward capture including the next 5 seconds
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/fwd.bag', post_duration_s: 5.0}" --feedback
```

## Forward captures

With `post_duration_s > 0`, the node copies the selected buffers when the
capture starts, keeps appending new messages to the copies for
`post_duration_s` seconds, then writes the bag. A topic's buffer limits only
need to cover the pre-trigger part. Messages arriving during the wait are
held outside `default_memory_limit` and `total_memory_limit` until the bag
is written, so a long forward capture of a heavy topic grows memory for its
whole duration. A topic with `include_post_trigger: false` gets only the
copy taken at the start.

Canceling the goal ends the wait early and writes what was collected so far.
The result has `success: false` and the bag is saved at `<filename>.partial`.

## Time windows

Buffered messages carry the time the snapshotter received them, and that is
their bag timestamp. `start_time`, `stop_time` and interval mode compare
against it.

A topic with no age limit (`default_duration_limit`, `duration` or
`duration_s` of `-1`) always writes its whole buffer: `start_time`,
`stop_time` and the interval window are ignored for it. With the default
`default_duration_limit` of `-1`, this applies to every topic that does not
set its own limit.

## Timestamps

When a goal sets `start_time` or `stop_time`, a topic with
`override_old_timestamps: true` or `old_messages_to_keep > 0` writes its
messages that are older than the window with `start_time` as their bag
timestamp. A goal with both times at `0` keeps every message's own
timestamp.

## Status topics

`snapshot_state` fields: `recording`, `active_capture_count`,
`buffered_topic_count`, `buffered_topics`, `buffered_window_s` (longest
oldest-to-newest span of any buffer), and the most recently finished
capture's outcome (`has_last_capture`, `last_capture_success`,
`last_capture_message`, `last_capture_stamp`). It is published when a goal
is accepted, a capture finishes, buffering is paused or resumed, or a new
profile topic starts buffering. It uses volatile QoS with depth 1, so a late
subscriber sees nothing until the next change.

`snapshot_capture_event` (depth 10) carries one message per finished
capture: `filename`, `profile`, `success`, `message`, `topics_written`,
`duration`, `stamp`.

## Pause and resume

`enable_snapshot` with `data: false` stops buffering; it always succeeds.
`data: true` resumes buffering, and is refused with `success: false` while a
capture is in progress. Resuming can discard data buffered before the pause.

## Operational notes

- Memory: a message larger than its topic's memory limit is dropped with a
  warning. When `total_memory_limit` is reached, the oldest messages of the
  largest buffer are evicted to make room.
- If a topic's receive time goes backwards (for example, a looping bag
  replay with simulated time), that topic's buffer is cleared.
- When `topics` is empty, the graph is polled every second and each new
  topic is buffered with the default limits and `DEFAULT` QoS. A topic
  advertised with more than one type stops that polling pass, so topics
  after it are not picked up.
- The `snapshotter` executable runs the node with intra-process
  communication enabled.

## Test

```bash
colcon test --packages-select rosbag2_snapshot --event-handlers console_direct+
colcon test-result --verbose
```

The tests are plain gtests and need no running graph.
