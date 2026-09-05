# rosbag2_snapshot

A `rosbag2` analogue of [`rosbag_snapshot`](https://github.com/ros/rosbag_snapshot): it subscribes to topics and keeps a rolling buffer of recent messages, like a dash cam. Run it in the background and, when something interesting happens, dump the recent past to disk on demand — no need to run `rosbag record` continuously (which live testing usually can't afford disk-space-wise).

## Usage

```
$ ros2 run rosbag2_snapshot snapshotter
```

There is no separate client binary. Requests to write a bag, pause/resume buffering, or cancel a live capture go through the `trigger_snapshot` action and `enable_snapshot` service the node exposes (see [Client](#client)).

### Example param file

```yaml
/**:
  ros__parameters:
    default_duration_limit: 10.0     # [Optional, default=-1] Max age (s) of the buffer, per topic
    default_memory_limit: 64.0       # [Optional, default=300] Max buffer size (MB), per topic
    total_memory_limit: 256.0        # [Optional, default=0] Shared cap (MB) across every topic combined. 0 = no shared cap
    max_post_duration_s: 300.0       # [Optional, default=300] Upper bound on a goal's post_duration_s (forward
                                      # capture window). <=0 disables forward captures. See "Forward (live) captures"
    interval_single_msg_types:       # [Optional] Extra types narrowed to a single message by
      - "sensor_msgs/msg/NavSatFix"  # interval_mode_single_msg (see TriggerSnapshot msg), on top of CameraInfo,
                                      # ImageMarker and compressed Image, which always get this treatment. A type
                                      # needs a real std_msgs/Header with a stamp to qualify; others are ignored
                                      # with a warning
    topics: ["/topic1", "/topic2"]   # [Optional] Topics to buffer. Empty = buffer everything
    topic_details:
      /topic1:
        type: "sensor_msgs/msg/NavSatFix"  # [Required if the topic is listed] Topic type
      /topic2:
        type: "sensor_msgs/msg/Odometry"
        duration: 15.0                # [Optional] Override the duration limit; inherit the memory limit
      /topic3:
        type: "sensor_msgs/msg/Image"
        duration: 2.0                 # [Optional] Override both limits
        memory: -1                    # Override the memory limit, in bytes (default_memory_limit above is in
                                       # MB). Negative = no limit
    capture_profiles_dir: "/path/to/profiles"  # [Optional] See "Capture profiles"
```

### Old timestamps

`override_old_timestamps` and `old_messages_to_keep` (per-topic settings) only matter when a request sets a real `start_time` or `stop_time`. With both left at zero (the default — "save everything buffered"), every message keeps its own timestamp regardless of these settings.

### Image compression

Set per topic via `topic_details.<topic>.compression.*` params — only for topics explicitly listed in `topics`/`topic_details` with `type: sensor_msgs/msg/Image` (not for auto-discovered or capture-profile topics):

| Param | Meaning |
|---|---|
| `compression.enabled` | Turn compression on for this topic |
| `compression.format` | `jpg` (default, quality 95) or `png` (default compression level 3) |
| `compression.jpg_quality` | 0-100 |
| `compression.png_compression` | 0-9 |

Setting `use_h264: true` on a `TriggerSnapshot` goal re-encodes that request's image topics as H264 through `FFMPEGEncoder` instead of using the topic's configured jpg/png setting; encoder params live under `h264.*` (codec, profile, preset, bitrate, etc. — see `ffmpeg_encoding/ffmpeg_encoder.hpp`). H264 support is a compile-time option (`ROSBAG2_SNAPSHOT_HAVE_H264`); a build without it falls back to the topic's jpg/png setting. `TopicDetails.h264_throttle_skip` (int-bool) skips throttling for a topic while H264 is active.

### Capture profiles

`capture_profiles_dir` points at a directory of `<name>.yaml` files, each a named, selectable topic set — independent of and combinable with the `topics`/`topic_details` list above.

```yaml
# capture_profiles/sensors.yaml
topics:
  - name: /imu
    max_rate_hz: 10.0        # [Optional] Downsample to at most one message per 1/max_rate_hz. Omit to keep every message
  - name: /odom
  - name: /camera/image_raw
    type: "sensor_msgs/msg/Image"  # [Optional] Same as topic_details above. If omitted, type is resolved from
    qos: "SENSOR_DATA"             # the ROS graph at subscribe time and QoS matches what publishers offer
                                    # (like `ros2 bag record`) -- retried on a timer until the publisher appears
    include_post_trigger: false    # [Optional, default=true] In a forward capture, keep this topic's pre-trigger
                                    # buffer but drop what arrives after the trigger. No effect outside a forward
                                    # capture. Distinct from "forward capture" (the post_duration_s mode) -- this
                                    # is a per-topic participation switch, not a mode switch.
```

A profile can nest others via `include`, so one `TriggerSnapshot.profile` selection can pull in several profiles at once:

```yaml
# capture_profiles/incident.yaml
include: [sensors, video]  # a single name also works: include: sensors
topics:                    # [Optional] not needed if this profile only combines others
  - name: /odom
    max_rate_hz: 5.0       # own entry overrides the inherited one (sensors' /odom) by name
  - name: /extra_topic
```

Rules:
- Only `topics` are inherited. Includes are merged in list order (later overrides earlier on a shared topic), and this profile's own `topics` always win over anything inherited.
- A profile that includes an unknown profile, ends up with no topics, or is part of an include cycle is dropped (logged as a startup warning) — the rest of the directory still loads.
- Every profile topic is buffered continuously from startup, same as the `topics` list — there's no "subscribe only while active" mode (a late VOLATILE subscription would miss everything published before it connected).
- If two independent profiles name the same topic, the one that sorts first alphabetically wins that topic's type/qos/max_rate_hz.

Select a profile via `TriggerSnapshot.profile` (`""` = today's behavior: the request's `topics`, or everything buffered). A named profile's `max_rate_hz` always applies, regardless of `throttle_msgs`. An unknown profile name is rejected at goal-acceptance time.

`rosbag_preset_profile` (in the param file) is unrelated — it's the rosbag2 storage compression preset (e.g. `zstd_small`), not a capture profile.

### Concurrent captures & atomic writes

Multiple `TriggerSnapshot` goals for *different* filenames may run at once — a real usage pattern (a client may schedule several captures together). The only concurrency rejection is a second goal for a filename already being written, since two writers on the same output would corrupt it; that goal is rejected at acceptance time and the filename frees up once the first capture finishes.

Every capture writes to a temporary file next to the requested name, then renames into place — a half-written file never appears at the requested name.

If a capture is canceled or a topic fails mid-write, the bag is still saved (never deleted) but as `<filename>.partial`, so a partial recording can't be mistaken for a complete one. `success` on the action result and the matching `snapshot_capture_event` message are the authoritative signal that a capture finished — don't infer it from a file existing on disk. A file only stays under `.partial` if the writer couldn't close cleanly (should be rare); a later request for that same filename replaces it.

### Forward (live) captures

By default a goal writes immediately from whatever's already buffered. Setting `post_duration_s` (float, seconds) turns it into a forward/live capture: writing is deferred until that many seconds after the goal is accepted, so the bag also includes messages that arrive after the trigger. Every buffered topic keeps being buffered by its normal subscription throughout — nothing new is subscribed, so the topic's `duration_limit`/`memory_limit` must be large enough to span from `start_time` through `trigger_time + post_duration_s`.

Canceling the goal (e.g. Ctrl-C on `ros2 action send_goal`, or `cancel_goal_async()`) ends the wait early and finalizes the bag with whatever was buffered so far — there's no separate "stop" call.

`max_post_duration_s` (node param, default `300.0`) caps how long a forward capture may run; a goal exceeding it is rejected at acceptance time. Set it to `0` or negative to disable forward captures entirely — callers that never set `post_duration_s` (default `0.0`) are unaffected either way.

### Status & capture-event topics

Independent of the per-request action feedback/result:

- **`snapshot_state`** (`rosbag2_snapshot_msgs/msg/SnapshotState`): `recording`, `active_capture_count`, `buffered_topic_count`, and the outcome of the most recently *finished* capture of any filename. Published on every state change (goal accepted/rejected, capture finalized, pause/resume, a new profile topic buffered).
- **`snapshot_capture_event`** (`rosbag2_snapshot_msgs/msg/SnapshotCaptureEvent`): one message per completed capture (`filename`, `profile`, `success`, `message`, `topics_written`, `duration`, `stamp`), so a consumer that needs a *specific* capture's outcome doesn't have to race the rolled-up state topic.

### Client

`trigger_snapshot` is a ROS 2 action (not a service), served directly by the `snapshotter` node.

###### Snapshot of all buffered topics

```
$ ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: 'snapshot.bag'}"
```

###### Snapshot of a named capture profile

```
$ ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: 'snapshot.bag', profile: 'sensors'}"
```

###### Forward (live) capture including the next 5 seconds

```
$ ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: 'snapshot.bag', post_duration_s: 5.0}" --feedback
```

###### Pause/resume buffering (a plain service, not the action)

```
$ ros2 service call /enable_snapshot std_srvs/srv/SetBool "{data: false}"  # false = pause, true = resume
requester: making request: std_srvs.srv.SetBool_Request(data=False)

response:
std_srvs.srv.SetBool_Response(success=True, message='')
```
