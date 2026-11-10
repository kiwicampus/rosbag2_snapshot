// Copyright (c) 2026, Kiwibot
// Differences between the Iron (rosbag2 0.22) and Jazzy (rosbag2 0.26) APIs.
// ROSBAG2_SNAPSHOT_JAZZY_API is set by CMakeLists.txt from rosbag2_storage's version.
#ifndef ROSBAG2_SNAPSHOT__ROSBAG2_COMPAT_HPP_
#define ROSBAG2_SNAPSHOT__ROSBAG2_COMPAT_HPP_

#ifdef ROSBAG2_SNAPSHOT_JAZZY_API
#include <rosbag2_storage/qos.hpp>
namespace rosbag2_snapshot
{
using Rosbag2QoS = rosbag2_storage::Rosbag2QoS;
}  // namespace rosbag2_snapshot
// SerializedBagMessage receive time field
#define ROSBAG2_SNAPSHOT_BAG_MSG_STAMP recv_timestamp
#else
#include <rosbag2_transport/qos.hpp>
namespace rosbag2_snapshot
{
using Rosbag2QoS = rosbag2_transport::Rosbag2QoS;
}  // namespace rosbag2_snapshot
#define ROSBAG2_SNAPSHOT_BAG_MSG_STAMP time_stamp
#endif

#endif  // ROSBAG2_SNAPSHOT__ROSBAG2_COMPAT_HPP_
