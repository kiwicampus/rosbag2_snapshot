/*
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

#ifndef ROSBAG2_SNAPSHOT__CAPTURE_PROFILES_HPP_
#define ROSBAG2_SNAPSHOT__CAPTURE_PROFILES_HPP_

#include <map>
#include <string>
#include <vector>

namespace rosbag2_snapshot
{

// One topic entry inside a capture profile file. type/qos are optional: left
// empty, the topic's type and QoS are resolved from the ROS graph at
// subscribe time instead (see TopicResolver).
struct ProfileTopicSpec
{
  std::string name;
  std::string type;
  std::string qos;
  // When this profile is selected in a TriggerSnapshot request, messages for
  // this topic are throttled to at most one per 1/max_rate_hz. 0 = no limit.
  double max_rate_hz = 0.0;
  // false = keep this topic's pre-trigger buffer in a forward capture, but
  // don't include what arrives after the trigger. true (default) records
  // both. No effect outside a forward capture. Named apart from "forward"
  // (the capture-level post_duration_s mode) since this is a per-topic
  // participation switch, not a mode switch.
  bool include_post_trigger = true;
};

struct CaptureProfile
{
  std::string name;
  // Names of other profiles this one nests, in order. Resolved by
  // loadProfilesDir(): by the time a CaptureProfile is handed out, `topics`
  // already holds the fully merged list (each included profile's topics, in
  // include order, then this profile's own topics -- later entries override
  // an earlier one of the same name). Kept here only for inspection; nothing
  // downstream needs to re-resolve it.
  std::vector<std::string> includes;
  std::vector<ProfileTopicSpec> topics;
};

// Every profile loaded from capture_profiles_dir, keyed by name (the file's
// stem).
struct ProfileSet
{
  std::map<std::string, CaptureProfile> profiles;

  const CaptureProfile * find(const std::string & name) const;
};

struct ProfileParseResult
{
  // False only if capture_profiles_dir itself could not be read. A malformed
  // individual profile file does not set this to false; it is skipped and
  // reported in warnings instead, so one bad file doesn't take the rest of
  // the directory down.
  bool ok = true;
  std::vector<std::string> warnings;
  ProfileSet profiles;
};

// Loads every "<name>.yaml" file directly inside dir as one profile, named
// after its filename stem. An empty dir is a no-op (capture_profiles_dir is
// optional).
ProfileParseResult loadProfilesDir(const std::string & dir);

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__CAPTURE_PROFILES_HPP_
