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

#ifndef ROSBAG2_SNAPSHOT__STAGING_PATH_HPP_
#define ROSBAG2_SNAPSHOT__STAGING_PATH_HPP_

#include <filesystem>

namespace rosbag2_snapshot
{

// Staging path for a bag's atomic write: same directory/filesystem as
// final_path (so the rename in Snapshotter::finalizeCapture() is atomic,
// never a fallback copy), just suffixed. ROS-free header: unit-testable with
// plain gtest, see test/test_staging_path.cpp.
inline std::filesystem::path stagingPathFor(const std::filesystem::path & final_path)
{
  std::filesystem::path staging_path = final_path;
  staging_path += ".tmp";
  return staging_path;
}

// Where a capture is saved if it didn't fully complete (canceled, or a topic
// failed to write) but the bag writer still closed the file cleanly. Always
// distinct from final_path, so a partial capture is never mistaken for a
// complete one just because a file exists at the requested name -- see
// "Concurrent captures & atomic writes" in the README.
inline std::filesystem::path partialPathFor(const std::filesystem::path & final_path)
{
  std::filesystem::path partial_path = final_path;
  partial_path += ".partial";
  return partial_path;
}

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__STAGING_PATH_HPP_
