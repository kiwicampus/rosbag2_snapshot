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

#ifndef ROSBAG2_SNAPSHOT__FORWARD_CAPTURE_HPP_
#define ROSBAG2_SNAPSHOT__FORWARD_CAPTURE_HPP_

namespace rosbag2_snapshot
{

// True if this goal asked for a forward (live) capture at all -- deliberately
// kept in its own ROS-free header (no rclcpp/etc.) so it's unit-testable with
// plain gtest -- see test/test_forward_capture.cpp -- same pattern as
// staging_path.hpp.
inline bool isForwardCaptureRequest(double post_duration_s)
{
  return post_duration_s > 0.0;
}

// True if a forward request is within the node's configured cap.
// max_post_duration_s <= 0 means forward captures are disabled entirely (not
// "unlimited") -- a node must be explicitly opted into this feature.
inline bool forwardCaptureWithinLimit(double post_duration_s, double max_post_duration_s)
{
  return isForwardCaptureRequest(post_duration_s) &&
         max_post_duration_s > 0.0 &&
         post_duration_s <= max_post_duration_s;
}

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__FORWARD_CAPTURE_HPP_
