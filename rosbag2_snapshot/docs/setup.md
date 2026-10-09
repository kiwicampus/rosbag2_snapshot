# Setting up rosbag2_snapshot

The `snapshotter` node buffers topics in memory and writes them to an MCAP
bag when a `TriggerSnapshot` goal arrives. The interfaces are listed in the
[README](../../README.md#interfaces).

## Prerequisites

| Dependency | Notes |
|---|---|
| `rclcpp`, `rclcpp_action`, `rclcpp_components`, `rmw`, `rosidl_typesupport_introspection_cpp` | Node, action server, component, header-stamp introspection |
| `std_srvs`, `sensor_msgs`, `visualization_msgs` | Interface types |
| `rosbag2_cpp`, `rosbag2_compression`, `rosbag2_compression_zstd`, `rosbag2_transport` | Bag writing, storage presets, QoS adaptation |
| rosbag2 MCAP storage plugin (`rosbag2_storage_mcap`) | Bags are always written with storage id `mcap`. Not listed in `package.xml` |
| `cv_bridge`, OpenCV | JPG/PNG image compression |
| `yaml-cpp` | Capture profile files |
| `pkg-config` | Required by CMake, with or without H264 |
| `foxglove_msgs` | H264 output type. Listed in `package.xml`, so rosdep installs it |
| FFmpeg development libraries (`libavcodec-dev`, `libavutil-dev`, `libswscale-dev`, `libswresample-dev`) | H264 only. Not in `package.xml`: rosdep does not install them. The default encoder needs an FFmpeg built with `libx264` |

## Build

```bash
colcon build --symlink-install --packages-up-to rosbag2_snapshot
```

H264 is built only when CMake finds both `foxglove_msgs` and the four FFmpeg
libraries; otherwise it prints a warning and builds without H264. FFmpeg is
found with pkg-config, through an exported `PKG_CONFIG_PATH` or the CMake
variable `FFMPEG_PKGCONFIG`, which is searched first. For an FFmpeg outside the
default search path:

```bash
colcon build --symlink-install --packages-up-to rosbag2_snapshot \
  --cmake-args -DFFMPEG_PKGCONFIG=/opt/ffmpeg/lib/pkgconfig
```

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

This writes `/tmp/snapshot.bag/snapshot.bag_0.mcap` and
`/tmp/snapshot.bag/metadata.yaml`. `param/single_topic.params.yaml` and
`param/multiple_topics.params.yaml` in the package source are further
examples. They are not installed.

### As a component

The node is registered as `rosbag2_snapshot::Snapshotter`:

```bash
ros2 run rclcpp_components component_container
ros2 component load /ComponentManager rosbag2_snapshot rosbag2_snapshot::Snapshotter \
  -p default_duration_limit:=30.0 -e use_intra_process_comms:=true
```

`use_intra_process_comms` matches the `snapshotter` executable, which always
enables it. In a launch file, pass the params file to the `ComposableNode`'s
`parameters`.

## Node parameters

Parameters are strictly typed: a value of the wrong type (for example `30`
for a `double`) fails node startup.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `default_duration_limit` | double | `-1.0` | Per-topic buffer age limit, seconds. `-1` = no age limit (see [Buffer limits](#buffer-limits)) |
| `default_memory_limit` | double | `300.0` | Per-topic buffer size limit, MB (1 MB = 1,000,000 bytes; fractions allowed). Negative = no limit. `0` drops every message of the topics that inherit it |
| `total_memory_limit` | double | `0.0` | Cap across all buffers combined, MB (fractions allowed). `<= 0` = no shared cap |
| `max_post_duration_s` | double | `300.0` | Longest accepted `post_duration_s`. `<= 0` disables forward captures |
| `rosbag_preset_profile` | string | `"zstd_small"` | MCAP storage preset used when a goal leaves its own empty: `none`, `fastwrite`, `zstd_fast` or `zstd_small`. Any other value makes every goal using it abort |
| `interval_single_msg_types` | string[] | `[]` | Extra message types narrowed to one message by `interval_mode_single_msg` |
| `capture_profiles_dir` | string | `""` | Directory of capture profiles. Empty = no profiles |
| `topics` | string[] | `[]` | Topics to buffer, each configured under `topic_details`. Empty = buffer every topic in the graph |
| `topic_details.<topic>.*` | | | Per-topic settings, below |
| `h264.*` | | | H264 encoder settings, below |

Leaving `topics` empty buffers every topic, even when `capture_profiles_dir`
is set. To buffer only profile topics, list at least one topic in `topics`.

### Per-topic settings (`topic_details.<topic>`)

Only topics listed in `topics` read these keys.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `type` | string | required | Message type, e.g. `sensor_msgs/msg/Image` |
| `qos` | string | `DEFAULT` | `DEFAULT` (reliable, depth 5), `SENSOR_DATA` (best effort, depth 5) or `TRANSIENT_LOCAL` (depth 5). An unknown value logs an error and uses `DEFAULT` |
| `duration` | double | `default_duration_limit` | Buffer age limit, seconds. `-1` = no limit, `0` = inherit |
| `memory` | double | `default_memory_limit` | Buffer size limit, **bytes**. Negative = no limit, `0` = inherit |
| `throttle_period` | double | `-1.0` | Minimum seconds between written messages, applied when the goal sets `throttle_msgs` |
| `queue_depth` | int | `-1` | Write at most the newest N messages in range. `<= 0` = no cap |
| `old_messages_to_keep` | int | `-1` | Also write up to N messages from before `start_time` |
| `override_old_timestamps` | bool | `false` | See [Timestamps](#timestamps) |
| `h264_throttle_skip` | bool | `false` | Ignore `throttle_period` while the topic is written as H264 |
| `compression.enabled` | bool | `false` | Compress this topic when written. Read for `sensor_msgs/msg/Image` topics only |
| `compression.format` | string | `jpg` | `jpg`, `jpeg` or `png`. Any other value, `h264` included, disables compression with an error |
| `compression.jpg_quality` | int | `95` | 0 to 100 |
| `compression.png_compression` | int | `3` | 0 to 9 |

A compressed topic is written as `sensor_msgs/msg/CompressedImage`.
`throttle_period`, `queue_depth` and `old_messages_to_keep` do not apply in
interval mode.

### H264 encoder (`h264.*`)

One set for the whole node, H264 builds only. The node declares these
parameters when it creates its first encoder, so they do not exist before
that. Every profile topic gets an encoder, and so does a `topic_details`
image topic that sets `compression.enabled` (and `compression.format` when
it is `true`).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `h264.encoding` | string | `"libx264"` | FFmpeg encoder name. Encoders containing `vaapi` open a VAAPI device; `h264_nvmpi` requires an image width that is a multiple of 64 |
| `h264.profile` | string | `""` | Encoder `profile` option. Empty = not set |
| `h264.preset` | string | `"ultrafast"` | Encoder `preset` option |
| `h264.tune` | string | `"zerolatency"` | Encoder `tune` option |
| `h264.delay` | string | `""` | Encoder `delay` option. Empty = not set |
| `h264.qmax` | int | `10` | Maximum quantizer, 0 (best) to 63 (worst) |
| `h264.bit_rate` | int64 | `8242880` | Target bit rate, bit/s |
| `h264.gop_size` | int64 | `15` | Frames between keyframes |
| `h264.pixel_format` | string | `""` | FFmpeg pixel format name, used by VAAPI encoders only. Empty = the encoder's preferred format |

A width that is not a multiple of 32 logs a warning. An encoder that fails to
open fails the capture.

A topic written as H264 is stored as `foxglove_msgs/msg/CompressedVideo`.
H264 applies only to an image topic that is being compressed: through
`compression.enabled: true`, a profile `compression` other than `none`, or a
goal entry with `use_compression: 1`. It is selected per goal with
`use_h264`, per profile topic with `compression: h264`, or per goal topic
with `format: h264`.

Each capture opens a fresh encoder and writes only the frames that produce a
packet. Settings that make the encoder buffer frames (a slower preset, a tune
other than `zerolatency`) therefore drop the first frames of every capture.
The defaults emit one packet per frame.

A topic without an encoder falls back to JPG/PNG. That covers every topic
in a build without H264, topics found by all-topics discovery, and
`topic_details` image topics that do not set `compression.enabled`, or set
it `true` without `compression.format`. For such a topic, `use_h264` writes
the topic's own JPG/PNG setting (JPG at `95` when it has none), and
`compression: h264` or `format: h264` writes JPG at the topic's
`jpg_quality` (`95` for a PNG topic or when none is configured).

FFmpeg's own log output (encoder setup and statistics) appears only if the
node logger is at DEBUG when an encoder is created. Its warnings and errors
always appear. Changing the log level later has no effect on it.

## Capture profiles

`capture_profiles_dir` holds one `<name>.yaml` file per profile; the file
stem is the profile name. Only `.yaml` files directly inside the directory are
read. A goal selects a profile by name in `profile`.

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

Profile files are plain YAML, not ROS parameters, so a `double` key accepts
`30` as well as `30.0`.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | Topic name |
| `type` | string | resolved from the graph | Message type |
| `qos` | string | adapted to the publishers' offered QoS | `DEFAULT`, `SENSOR_DATA` or `TRANSIENT_LOCAL` |
| `duration_s` | double | `default_duration_limit` | Buffer age limit, seconds. `> 0`, or `-1` for no limit |
| `memory_mb` | double | `default_memory_limit` | Buffer size limit, MB. `> 0` |
| `max_rate_hz` | double | `0` (every message) | Write at most one message per `1/max_rate_hz` seconds. `>= 0`. Not applied in interval mode |
| `include_post_trigger` | bool | `true` | In a forward capture, `false` writes only what was buffered at the trigger |
| `compression` | string | topic's own setting | `jpg`, `png`, `h264` or `none` (`jpeg` is rejected). Applies to image topics; other types are written uncompressed with a warning |
| `compression_quality` | int | `95` (jpg), `3` (png) | jpg 0 to 100, png 0 to 9. Requires `compression: jpg` or `png` |
| `override_old_timestamps` | bool | topic's own setting | As in `topic_details` |
| `queue_depth` | int | topic's own setting | As in `topic_details`. `> 0` |
| `old_messages_to_keep` | int | topic's own setting | As in `topic_details`. `> 0` |
| `h264_throttle_skip` | bool | topic's own setting | As in `topic_details` |

`type`, `qos`, `duration_s` and `memory_mb` set how the topic is buffered.
The other keys apply when the profile is selected.

### Profile rules

- Every profile topic is buffered from startup, whether or not a profile is
  selected. A topic whose type or QoS cannot be resolved yet (no publisher)
  is retried every second. A topic that sets both `type` and `qos` needs no
  publisher to be subscribed.
- A topic already listed in `topics` keeps its `topic_details` buffering.
  When several profiles name the same topic, the profile whose name sorts
  first sets its buffering (`type`, `qos`, `duration_s`, `memory_mb`).
- `include` accepts a name or a list. Only `topics` are inherited. Includes
  merge in list order, later entries replacing earlier ones by topic name,
  and the profile's own `topics` replace inherited ones.
- A profile is dropped, with a startup warning, when its file is not valid
  YAML, a value cannot be read as its key's type (for example
  `queue_depth: 1.5`), it fails the checks in the table above, it includes
  an unknown or dropped profile, it is part of an include cycle, or it ends
  up with no topics. The rest still load.
- A `capture_profiles_dir` that is not a directory logs a warning and loads
  no profiles.

## Triggering a capture

Send a `trigger_snapshot` goal. Every field is optional except `filename`.

### Goal fields

| Field | Default | Meaning |
|---|---|---|
| `filename` | required | Output path. Must end in `.bag` or `.mcap`, else the goal is rejected |
| `use_flat_output` | `false` | `false`: write a bag directory at `filename`. `true`: write a single `.mcap` file at `filename` |
| `profile` | `""` | Capture profile to write. Empty = use `topics`. An unknown name is rejected |
| `topics` | `[]` | [`TopicDetails`](#goal-topic-entries) entries. Empty = every buffered topic. Ignored when `profile` is set |
| `start_time` | `0` | Earliest message to write. `0` = oldest buffered |
| `stop_time` | `0` | Latest message to write. `0` = newest buffered |
| `post_duration_s` | `0.0` | `> 0` makes a [forward capture](#forward-captures) |
| `throttle_msgs` | `false` | Apply each topic's `throttle_period`. A profile's `max_rate_hz` applies regardless. Neither applies in interval mode |
| `use_h264` | `false` | Write compressed image topics as H264 |
| `rosbag_preset_profile` | `""` | MCAP storage preset (`none`, `fastwrite`, `zstd_fast`, `zstd_small`). Empty = the node parameter |
| `use_interval_mode` | `false` | Write `[msg_timestamp - interval_mode_tolerance, msg_timestamp + interval_mode_tolerance]` instead of `start_time`/`stop_time` |
| `msg_timestamp` | `0` | Interval center |
| `interval_mode_tolerance` | `0.0` | Interval half-width, seconds |
| `interval_mode_single_msg` | `false` | In interval mode, write one message per topic for `sensor_msgs/msg/CameraInfo`, `visualization_msgs/msg/ImageMarker`, compressed `sensor_msgs/msg/Image` topics and `interval_single_msg_types`: the one whose header stamp equals `msg_timestamp`, else the closest. Types without a `std_msgs/Header` keep the whole interval |

A requested topic that is not buffered is skipped with a warning. The goal is
also rejected when `post_duration_s` exceeds `max_post_duration_s` or forward
captures are disabled, or when a capture to the same `filename` is still in
progress. Captures to different filenames run concurrently.

### Goal topic entries

Each `TopicDetails` entry overrides a buffered topic's settings for this
capture. Entries are matched by `name`; `type` is ignored. A field left at
`-1` or `""` keeps the topic's configured value.

| Field | Type | Inherit value | Meaning |
|---|---|---|---|
| `name` | string | | Buffered topic name |
| `throttle_period` | float32 | `-1.0` | As in `topic_details` |
| `h264_throttle_skip` | int8 | `-1` | `0` or `1` |
| `override_old_timestamps` | int8 | `-1` | `0` or `1` |
| `queue_depth` | int32 | `-1` | `0` = no cap |
| `old_messages_to_keep` | int32 | `-1` | `0` = none |
| `use_compression` | int8 | `-1` | `0` or `1` |
| `format` | string | `""` | `jpg`, `jpeg`, `png` or `h264`. Any other value disables compression with a warning |
| `jpg_quality` | int32 | `-1` | 0 to 100. Read only with `format: jpg` or `jpeg` in the same entry |
| `png_compression` | int32 | `-1` | 0 to 9. Read only with `format: png` in the same entry |
| `include_post_trigger` | int8 | `-1` | `0` or `1` |

Changing `format` without the matching quality field keeps the topic's current
value when the topic already uses that format, and otherwise uses the default
(`95` for JPG, `3` for PNG). A selected profile applies its topic
keys as these same overrides.

`use_compression: 1` with no `format`, on a topic that has no compression
format of its own, writes JPG at quality `95`.

### Result and feedback

| Field | Meaning |
|---|---|
| result `success` | `true` only for a complete capture |
| result `message` | Saved path on success, else the reason |
| feedback `progress` | Percent complete |
| feedback `duration` | Seconds since the capture started |
| feedback `message` | Current step |

Feedback is published every 0.5 s while a forward capture waits, and after
each topic is written.

`success` is the authoritative outcome. The action status is `SUCCEEDED`
for complete and failed captures and for a goal canceled during a forward
capture's wait, `CANCELED` for a goal canceled while writing, and `ABORTED`
when the output cannot be opened (an unknown storage preset, or a leftover
`<filename>.tmp` that cannot be removed, included) or the capture cannot
start.

### Output files

| Outcome | `use_flat_output: false` | `use_flat_output: true` |
|---|---|---|
| Complete | Directory `<filename>/` with `<name>_0.mcap` and `metadata.yaml` | File `<filename>`, replacing an existing file |
| Canceled, a topic failed to write, or the capture crashed | Directory `<filename>.partial/` with `<name>.partial_0.mcap` | File `<filename>.partial` |
| Writer failed to close, or the move failed | Left in the staging directory `<filename>.tmp/` | Same |

`<name>` is the last component of `filename`. For `filename: /tmp/snapshot.bag`
a complete capture is `/tmp/snapshot.bag/snapshot.bag_0.mcap` and a partial
one `/tmp/snapshot.bag.partial/snapshot.bag.partial_0.mcap`.

Data is staged in `<filename>.tmp` and moved into place when the writer
closes. `snapshot_capture_event.filename` gives the path actually written.

A capture first removes anything left at `<filename>.tmp`, file or
directory, and logs a warning naming it. That includes the data of an
earlier capture whose move failed. If the removal fails, the goal is aborted
with the error in `message`.

The move into `<filename>/` (or `<filename>.partial/`) fails when that
directory already exists and is not empty. The capture reports
`success: false` and the data stays in `<filename>.tmp` until the next
capture to that `filename`. Give every capture its own `filename`, or remove
`<filename>` before reusing one.

### Examples

```bash
# Every buffered topic
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/all.bag'}"

# A named profile, as a single file
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/sensors.mcap', profile: 'sensors', use_flat_output: true}"

# Forward capture including the next 5 seconds
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/fwd.bag', post_duration_s: 5.0}" --feedback

# One topic as JPG at quality 60, overriding its configuration
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/cam.bag', topics: [{name: '/camera/image_raw', use_compression: 1, format: 'jpg', jpg_quality: 60}]}"
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
The result has `success: false`, the action status is `SUCCEEDED`, and the
bag is saved at `<filename>.partial`.

## Buffer limits

Buffered messages carry the time the snapshotter received them, and that is
their bag timestamp. `start_time`, `stop_time` and interval mode compare
against it.

- Each buffered message counts its serialized size plus a fixed estimate of
  bookkeeping overhead toward the memory limits.
- With the defaults (`default_memory_limit` 300 MB, `total_memory_limit` 0,
  no age limit), every topic can grow to 300 MB. Set an age limit or
  `total_memory_limit` to bound the total.
- A message larger than its topic's memory limit is dropped with a warning.
  When `total_memory_limit` is reached, the oldest messages of the largest
  buffer are evicted to make room.
- A topic with no age limit (`default_duration_limit`, `duration` or
  `duration_s` of `-1`) always writes its whole buffer: `start_time`,
  `stop_time` and the interval window are ignored for it. With the default
  `default_duration_limit` of `-1`, this applies to every topic that does not
  set its own limit.
- The age limit is applied when a message arrives, measured back from that
  message. A topic that stops publishing keeps its last messages however old
  they get. A goal with neither `topics` nor `profile` also drops, from each
  topic, messages older than the age limit at the moment of the trigger. A
  goal that names topics does not.
- `old_messages_to_keep` takes effect only for a topic with an age limit, in
  a goal with a non-zero `start_time`, outside interval mode.
- A buffer is cleared only if its topic has an age limit. Such a buffer is
  cleared when its receive time goes backwards (for example, a looping bag
  replay with simulated time), and on resume (see
  [Pause and resume](#pause-and-resume)).

## Timestamps

When a goal sets `start_time` or `stop_time`, a topic with
`override_old_timestamps: true` or `old_messages_to_keep > 0` writes its
messages received before `start_time` with `start_time` as their bag
timestamp. A goal with both times at `0` keeps every message's own timestamp.

## Status topics

`snapshot_state` fields: `recording`, `active_capture_count`,
`buffered_topic_count`, `buffered_topics`, `buffered_window_s` (longest
oldest-to-newest span of any buffer), and the most recently finished
capture's outcome (`has_last_capture`, `last_capture_success`,
`last_capture_message`, `last_capture_stamp`). It is published once at
startup, and when a goal is accepted, a capture finishes or fails to start,
buffering is paused or resumed, or a profile topic starts buffering. A topic
added by all-topics discovery does not trigger it. It uses volatile QoS with
depth 1, so a late subscriber sees nothing until the next change.

`snapshot_capture_event` (reliable, volatile, depth 10) carries one message
per capture that ran. A goal aborted before writing (the output cannot be
opened, or the capture cannot start) publishes none; only `snapshot_state`
reports it.

| Field | Meaning |
|---|---|
| `filename` | Path actually written: `<filename>`, `<filename>.partial` or `<filename>.tmp` (see [Output files](#output-files)) |
| `profile` | The goal's profile, empty when none |
| `success`, `message` | Outcome; `message` is the path on success, the reason otherwise |
| `topics_written` | Topics the capture went through, skipped ones included |
| `duration`, `stamp` | Seconds since the capture started; publish time |
| `content_topics`, `content_message_counts` | Every topic the bag declares and the messages written on it (parallel arrays; 0 for a declared topic with none) |
| `first_message_stamp`, `last_message_stamp` | Receipt time of the first and last message written; zero when the bag holds none |

## Pause and resume

`enable_snapshot` with `data: false` stops buffering; it always succeeds.
`data: true` resumes buffering, and is refused with `success: false` while a
capture is in progress. On resume, a buffer whose topic has an age limit is
cleared when its oldest-to-newest span exceeds `default_duration_limit` (for
topics listed in `topics`) or `0` (profile and discovered topics).

## Verifying a capture

Watch the capture events in one terminal:

```bash
ros2 topic echo /snapshot_capture_event
```

Trigger a capture in another, then inspect the bag:

```bash
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/check.bag'}" --feedback
ros2 bag info /tmp/check.bag
```

The result should have `success: true`, and the event's `content_topics` and
`content_message_counts` should match what `ros2 bag info` lists. A topic
with a count of `0` was declared but had nothing in range. `/snapshot_state`
lists the buffered topics, from the next state change on (see
[Status topics](#status-topics)).

## Operational notes

- When `topics` is empty, the graph is polled every second and each new
  topic is buffered with the default limits, `DEFAULT` QoS and no
  compression settings. Discovery includes the node's own publishers. A
  topic advertised with no type or with more than one type is skipped, with
  an error logged on every poll.
- Each subscription requests topic statistics on `<topic>/statistics`.
  rclcpp versions whose generic subscriptions do not support topic
  statistics, ROS 2 Iron included, publish nothing there.
- The `snapshotter` executable runs the node in a single-threaded executor
  with intra-process communication enabled. Captures write on their own
  threads.

## Test

```bash
colcon test --packages-select rosbag2_snapshot --event-handlers console_direct+
colcon test-result --verbose
```

The tests are plain gtests and need no running graph. `test_ffmpeg_encoder`
is built only when H264 is.
