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

#ifndef ROSBAG2_SNAPSHOT__TIMESTAMP_OVERRIDE_HPP_
#define ROSBAG2_SNAPSHOT__TIMESTAMP_OVERRIDE_HPP_

#include <cstdint>

namespace rosbag2_snapshot
{

// True if a buffered message's own timestamp should be replaced with the
// request's start_time when writing it into a bag, rather than keeping the
// time it actually arrived. ROS-free header: unit-testable with plain gtest,
// see test/test_timestamp_override.cpp.
//
// Only true when both:
//  - the topic opted in (override_old_timestamps, or old_messages_to_keep
//    greater than zero), and
//  - the request specifies a real time window (start_time or stop_time set).
//    A request that leaves both at zero asks for "everything currently
//    buffered" -- there is no window to be "outside of", so every message
//    keeps its own timestamp regardless of the topic's settings. Every
//    forward (live) capture, and any request that doesn't set them, leaves
//    both at zero.
inline bool shouldOverrideOldTimestamp(
  bool override_old_timestamps,
  int old_messages_to_keep,
  bool start_time_specified,
  bool stop_time_specified,
  int64_t message_age_ns,
  int64_t bag_duration_ns)
{
  if (!(override_old_timestamps || old_messages_to_keep > 0)) {
    return false;
  }
  if (!start_time_specified && !stop_time_specified) {
    return false;
  }
  return message_age_ns > bag_duration_ns;
}

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__TIMESTAMP_OVERRIDE_HPP_
