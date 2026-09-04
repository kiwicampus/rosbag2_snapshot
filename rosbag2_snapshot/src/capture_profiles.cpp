/*
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

#include "rosbag2_snapshot/capture_profiles.hpp"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <string>

namespace rosbag2_snapshot
{

namespace
{

// A topic entry with the same name as an existing one in merged replaces it
// in place (last write wins); otherwise it's appended. Used both for
// include-order merging and for "own topics override inherited ones".
void mergeTopic(std::vector<ProfileTopicSpec> & merged, const ProfileTopicSpec & spec)
{
  auto existing = std::find_if(
    merged.begin(), merged.end(),
    [&spec](const ProfileTopicSpec & s) {return s.name == spec.name;});
  if (existing != merged.end()) {
    *existing = spec;
  } else {
    merged.push_back(spec);
  }
}

bool parseProfileFile(const std::filesystem::path & path, CaptureProfile & out, std::string & error)
{
  YAML::Node root;
  try {
    root = YAML::LoadFile(path.string());
  } catch (const std::exception & ex) {
    error = ex.what();
    return false;
  }

  CaptureProfile profile{};
  profile.name = path.stem().string();

  if (root["include"]) {
    if (root["include"].IsScalar()) {
      profile.includes.push_back(root["include"].as<std::string>());
    } else if (root["include"].IsSequence()) {
      for (const auto & node : root["include"]) {
        profile.includes.push_back(node.as<std::string>());
      }
    } else {
      error = "'include' must be a profile name or a list of profile names";
      return false;
    }
  }

  if (root["topics"]) {
    if (!root["topics"].IsSequence()) {
      error = "'topics' must be a list";
      return false;
    }
    for (const auto & node : root["topics"]) {
      if (!node["name"] || node["name"].as<std::string>().empty()) {
        error = "a topic entry is missing 'name'";
        return false;
      }

      ProfileTopicSpec spec{};
      spec.name = node["name"].as<std::string>();
      spec.type = node["type"] ? node["type"].as<std::string>() : "";
      spec.qos = node["qos"] ? node["qos"].as<std::string>() : "";
      spec.max_rate_hz = node["max_rate_hz"] ? node["max_rate_hz"].as<double>() : 0.0;
      spec.forward = node["forward"] ? node["forward"].as<bool>() : true;

      if (spec.max_rate_hz < 0.0) {
        error = "max_rate_hz for topic " + spec.name + " must be >= 0";
        return false;
      }

      profile.topics.push_back(spec);
    }
  }

  if (profile.topics.empty() && profile.includes.empty()) {
    error = "must have a non-empty 'topics' list and/or an 'include'";
    return false;
  }

  out = profile;
  return true;
}

enum class ResolveState { kUnresolved, kVisiting, kResolved, kFailed };

// Depth-first, memoized: resolves `name`'s final topic list (includes merged
// in order, then this profile's own topics overriding by name) into
// `resolved`. False (and no entry in `resolved`) if `name` doesn't exist,
// is part of an include cycle, or ends up with no topics -- in every case a
// human-readable reason is appended to `warnings` and the profile is
// dropped, the same "one bad entry doesn't take the rest down" policy
// parseProfileFile already uses per-file.
bool resolveProfile(
  const std::string & name,
  const std::map<std::string, CaptureProfile> & raw,
  std::map<std::string, ResolveState> & state,
  std::map<std::string, std::vector<ProfileTopicSpec>> & resolved,
  std::vector<std::string> & warnings)
{
  auto state_it = state.find(name);
  if (state_it != state.end()) {
    return state_it->second == ResolveState::kResolved;
  }

  auto raw_it = raw.find(name);
  if (raw_it == raw.end()) {
    warnings.push_back("include references unknown profile '" + name + "'");
    return false;
  }

  state[name] = ResolveState::kVisiting;

  std::vector<ProfileTopicSpec> merged;
  bool includes_ok = true;
  for (const auto & include_name : raw_it->second.includes) {
    auto include_state = state.find(include_name);
    if (include_state != state.end() && include_state->second == ResolveState::kVisiting) {
      warnings.push_back(
        "profile '" + name + "' dropped: include cycle via '" + include_name + "'");
      includes_ok = false;
      continue;
    }
    if (!resolveProfile(include_name, raw, state, resolved, warnings)) {
      includes_ok = false;
      continue;
    }
    for (const auto & spec : resolved[include_name]) {
      mergeTopic(merged, spec);
    }
  }

  if (!includes_ok) {
    state[name] = ResolveState::kFailed;
    return false;
  }

  for (const auto & spec : raw_it->second.topics) {
    mergeTopic(merged, spec);
  }

  if (merged.empty()) {
    warnings.push_back("profile '" + name + "' dropped: no topics once its includes are resolved");
    state[name] = ResolveState::kFailed;
    return false;
  }

  resolved[name] = merged;
  state[name] = ResolveState::kResolved;
  return true;
}

}  // namespace

const CaptureProfile * ProfileSet::find(const std::string & name) const
{
  auto it = profiles.find(name);
  return it == profiles.end() ? nullptr : &it->second;
}

ProfileParseResult loadProfilesDir(const std::string & dir)
{
  ProfileParseResult result{};
  if (dir.empty()) {
    return result;
  }

  std::error_code ec;
  if (!std::filesystem::is_directory(dir, ec) || ec) {
    result.ok = false;
    result.warnings.push_back("capture_profiles_dir '" + dir + "' is not a directory");
    return result;
  }

  std::map<std::string, CaptureProfile> raw;
  for (const auto & entry : std::filesystem::directory_iterator(dir, ec)) {
    if (ec) {
      break;
    }
    if (!entry.is_regular_file() || entry.path().extension() != ".yaml") {
      continue;
    }

    CaptureProfile profile{};
    std::string error{};
    if (!parseProfileFile(entry.path(), profile, error)) {
      result.warnings.push_back(entry.path().filename().string() + ": " + error);
      continue;
    }

    if (raw.count(profile.name) > 0) {
      result.warnings.push_back("duplicate profile name '" + profile.name + "', keeping the first one found");
      continue;
    }

    raw[profile.name] = profile;
  }

  // Resolved as a second pass over the whole directory, so a profile can
  // include one defined in another file regardless of file iteration order.
  std::map<std::string, ResolveState> state;
  std::map<std::string, std::vector<ProfileTopicSpec>> resolved_topics;
  for (const auto & entry : raw) {
    resolveProfile(entry.first, raw, state, resolved_topics, result.warnings);
  }

  for (const auto & entry : raw) {
    if (state[entry.first] == ResolveState::kResolved) {
      CaptureProfile final_profile = entry.second;
      final_profile.topics = resolved_topics[entry.first];
      result.profiles.profiles[entry.first] = final_profile;
    }
  }

  return result;
}

}  // namespace rosbag2_snapshot
