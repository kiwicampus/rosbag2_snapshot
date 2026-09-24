// Copyright (c) 2018-2021, Open Source Robotics Foundation, Inc., GAIA Platform, Inc., All rights reserved.  // NOLINT
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//    * Neither the name of the {copyright_holder} nor the names of its
//      contributors may be used to endorse or promote products derived from
//      this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#ifndef ROSBAG2_SNAPSHOT__PROFILE_TOPIC_DETAILS_HPP_
#define ROSBAG2_SNAPSHOT__PROFILE_TOPIC_DETAILS_HPP_

#include <string>

#include <rosbag2_snapshot_msgs/msg/topic_details.hpp>

#include "rosbag2_snapshot/capture_profiles.hpp"

namespace rosbag2_snapshot
{

// The TopicDetails override a selected profile's topic applies. Unset
// options stay -1 / "", so the topic's own configuration is kept.
inline rosbag2_snapshot_msgs::msg::TopicDetails profileTopicDetails(
  const ProfileTopicSpec & spec)
{
  rosbag2_snapshot_msgs::msg::TopicDetails msg{};
  msg.name = spec.name;
  msg.throttle_period = spec.max_rate_hz > 0.0 ? (1.0 / spec.max_rate_hz) : -1.0;
  msg.include_post_trigger = spec.include_post_trigger ? 1 : 0;
  if (spec.compression == "none") {
    msg.use_compression = 0;
  } else if (!spec.compression.empty()) {
    msg.use_compression = 1;
    msg.format = spec.compression;
    if (spec.compression == "jpg") {
      msg.jpg_quality = spec.compression_quality.value_or(95);
    } else if (spec.compression == "png") {
      msg.png_compression = spec.compression_quality.value_or(3);
    }
  }
  if (spec.override_old_timestamps.has_value()) {
    msg.override_old_timestamps = *spec.override_old_timestamps ? 1 : 0;
  }
  if (spec.queue_depth.has_value()) {
    msg.queue_depth = *spec.queue_depth;
  }
  if (spec.old_messages_to_keep.has_value()) {
    msg.old_messages_to_keep = *spec.old_messages_to_keep;
  }
  if (spec.h264_throttle_skip.has_value()) {
    msg.h264_throttle_skip = *spec.h264_throttle_skip ? 1 : 0;
  }
  return msg;
}

// Compression applies to sensor_msgs/msg/Image only; an unknown type is
// assumed to be one.
inline bool compressesTopicType(bool use_compression, const std::string & type)
{
  return use_compression && (type.empty() || type == "sensor_msgs/msg/Image");
}

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__PROFILE_TOPIC_DETAILS_HPP_
