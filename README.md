# rosbag2_snapshot

A `rosbag2` analogue of [`rosbag_snapshot`](https://github.com/ros/rosbag_snapshot).
The `snapshotter` node subscribes to topics and keeps a rolling in-memory
buffer of recent messages. On request it writes the buffered past, and
optionally a window after the request, to an MCAP bag.

Setup, the full configuration reference and operational notes are in
[`rosbag2_snapshot/docs/setup.md`](rosbag2_snapshot/docs/setup.md).

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
| `<topic>/statistics` | publisher | `statistics_msgs/msg/MetricsMessage` | Subscription statistics for each buffered topic |

## Quick start

```bash
ros2 run rosbag2_snapshot snapshotter --ros-args --params-file my_params.yaml
ros2 action send_goal /trigger_snapshot rosbag2_snapshot_msgs/action/TriggerSnapshot "{filename: '/tmp/snapshot.bag'}"
```

This writes every buffered topic to the bag directory `/tmp/snapshot.bag/`.
See [the setup guide](rosbag2_snapshot/docs/setup.md#triggering-a-capture)
for profiles, forward captures and the other goal fields.
