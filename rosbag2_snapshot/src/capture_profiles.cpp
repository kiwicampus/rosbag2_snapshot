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
      spec.include_post_trigger =
        node["include_post_trigger"] ? node["include_post_trigger"].as<bool>() : true;
      if (node["duration_s"]) {
        spec.duration_s = node["duration_s"].as<double>();
      }
      if (node["memory_mb"]) {
        spec.memory_mb = node["memory_mb"].as<double>();
      }
      spec.compression = node["compression"] ? node["compression"].as<std::string>() : "";
      if (node["compression_quality"]) {
        spec.compression_quality = node["compression_quality"].as<int>();
      }
      if (node["override_old_timestamps"]) {
        spec.override_old_timestamps = node["override_old_timestamps"].as<bool>();
      }
      if (node["queue_depth"]) {
        spec.queue_depth = node["queue_depth"].as<int>();
      }
      if (node["old_messages_to_keep"]) {
        spec.old_messages_to_keep = node["old_messages_to_keep"].as<int>();
      }
      if (node["h264_throttle_skip"]) {
        spec.h264_throttle_skip = node["h264_throttle_skip"].as<bool>();
      }

      if (spec.max_rate_hz < 0.0) {
        error = "max_rate_hz for topic " + spec.name + " must be >= 0";
        return false;
      }
      if (spec.duration_s.has_value() && *spec.duration_s <= 0.0 && *spec.duration_s != -1.0) {
        error = "duration_s for topic " + spec.name + " must be > 0, or -1 for no limit";
        return false;
      }
      if (!spec.compression.empty() && spec.compression != "jpg" &&
        spec.compression != "png" && spec.compression != "h264" && spec.compression != "none")
      {
        error = "compression for topic " + spec.name + " must be jpg, png, h264 or none";
        return false;
      }
      if (spec.compression_quality.has_value()) {
        const int max = spec.compression == "jpg" ? 100 : (spec.compression == "png" ? 9 : -1);
        if (max < 0 || *spec.compression_quality < 0 || *spec.compression_quality > max) {
          error = "compression_quality for topic " + spec.name +
            " needs jpg (0-100) or png (0-9) compression";
          return false;
        }
      }
      if (spec.queue_depth.has_value() && *spec.queue_depth <= 0) {
        error = "queue_depth for topic " + spec.name + " must be > 0";
        return false;
      }
      if (spec.old_messages_to_keep.has_value() && *spec.old_messages_to_keep <= 0) {
        error = "old_messages_to_keep for topic " + spec.name + " must be > 0";
        return false;
      }
      if (spec.memory_mb.has_value() && *spec.memory_mb <= 0.0) {
        error = "memory_mb for topic " + spec.name + " must be > 0";
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
// `resolved`. False (and no entry in `resolved`) if `name` doesn't exist, is
// part of an include cycle, includes a failed profile, or ends up with no
// topics.
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
