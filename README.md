# rosbag2_snapshot

A `rosbag2` analogue of [`rosbag_snapshot`](https://github.com/ros/rosbag_snapshot).
The `snapshotter` node subscribes to topics and keeps a rolling in-memory
buffer of recent messages. On request it writes the buffered past, and
optionally a window after the request, to an MCAP bag.

Features:

- Per-topic age and memory limits, plus an optional cap shared by all buffers
- Named capture profiles that select topics and their write settings
- Forward captures that also include messages arriving after the trigger
- Time-window and interval selection, throttling and per-topic queue depth
- JPG/PNG image compression, and H264 when built with FFmpeg
- A state topic and one event per finished capture

Setup, a minimal example, the full configuration reference and operational
notes are in [`rosbag2_snapshot/docs/setup.md`](rosbag2_snapshot/docs/setup.md).

## Packages

| Package | Contents |
|---|---|
| `rosbag2_snapshot` | The `snapshotter` executable and the `rosbag2_snapshot::Snapshotter` component |
| `rosbag2_snapshot_msgs` | `TriggerSnapshot.action`, `SnapshotState.msg`, `SnapshotCaptureEvent.msg`, `TopicDetails.msg` |

## Interfaces

Names are relative to the node's namespace. The default node name is
`snapshotter`.

| Name | Kind | Type | Purpose |
|---|---|---|---|
| `trigger_snapshot` | action server | `rosbag2_snapshot_msgs/action/TriggerSnapshot` | Write a bag. Cancel a goal to end a capture early |
| `enable_snapshot` | service | `std_srvs/srv/SetBool` | `false` pauses buffering, `true` resumes it |
| `snapshot_state` | publisher | `rosbag2_snapshot_msgs/msg/SnapshotState` | Node state, published on each change |
| `snapshot_capture_event` | publisher | `rosbag2_snapshot_msgs/msg/SnapshotCaptureEvent` | One message per finished capture |

To run the node, start from the
[minimal example](rosbag2_snapshot/docs/setup.md#minimal-example). Goal
fields, profiles and output paths are in
[Triggering a capture](rosbag2_snapshot/docs/setup.md#triggering-a-capture).
