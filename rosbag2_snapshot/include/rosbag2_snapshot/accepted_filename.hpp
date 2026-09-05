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

#ifndef ROSBAG2_SNAPSHOT__ACCEPTED_FILENAME_HPP_
#define ROSBAG2_SNAPSHOT__ACCEPTED_FILENAME_HPP_

#include <string>

namespace rosbag2_snapshot
{

namespace detail
{
inline bool endsWithLiteral(const std::string & value, const std::string & suffix)
{
  return value.size() >= suffix.size() &&
         value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}
}  // namespace detail

// A TriggerSnapshot goal's filename is used verbatim as the on-disk final
// path, so handle_goal() rejects anything not ending in ".bag" (the usual
// rosbag2 directory-mode convention) or ".mcap" (lets a use_flat_output=true
// caller, e.g. blackbox, pass its real destination filename straight
// through). ROS-free header: unit-testable with plain gtest, see
// test/test_accepted_filename.cpp.
inline bool hasAcceptedGoalFilename(const std::string & filename)
{
  return !filename.empty() &&
         (detail::endsWithLiteral(filename, ".bag") ||
          detail::endsWithLiteral(filename, ".mcap"));
}

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__ACCEPTED_FILENAME_HPP_
