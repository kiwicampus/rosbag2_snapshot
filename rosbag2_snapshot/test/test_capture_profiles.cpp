#include "rosbag2_snapshot/capture_profiles.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>

namespace
{

std::filesystem::path makeEmptyDir(const std::string & suffix)
{
  auto dir = std::filesystem::temp_directory_path() / ("capture_profiles_test_" + suffix);
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

void writeFile(const std::filesystem::path & path, const std::string & content)
{
  std::ofstream out(path);
  out << content;
}

}  // namespace

TEST(CaptureProfiles, LoadsValidProfiles)
{
  auto dir = makeEmptyDir("valid");
  writeFile(dir / "sensors.yaml", "topics:\n  - name: /imu\n    max_rate_hz: 10.0\n  - name: /odom\n");
  writeFile(
    dir / "video.yaml",
    "topics:\n  - name: /camera/image_raw\n    type: sensor_msgs/msg/Image\n    qos: SENSOR_DATA\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_TRUE(result.ok);
  EXPECT_TRUE(result.warnings.empty());
  ASSERT_EQ(result.profiles.profiles.size(), 2u);

  const auto * sensors = result.profiles.find("sensors");
  ASSERT_NE(sensors, nullptr);
  ASSERT_EQ(sensors->topics.size(), 2u);
  EXPECT_EQ(sensors->topics[0].name, "/imu");
  EXPECT_DOUBLE_EQ(sensors->topics[0].max_rate_hz, 10.0);
  EXPECT_TRUE(sensors->topics[0].include_post_trigger);
  EXPECT_EQ(sensors->topics[1].name, "/odom");
  EXPECT_DOUBLE_EQ(sensors->topics[1].max_rate_hz, 0.0);

  const auto * video = result.profiles.find("video");
  ASSERT_NE(video, nullptr);
  ASSERT_EQ(video->topics.size(), 1u);
  EXPECT_EQ(video->topics[0].type, "sensor_msgs/msg/Image");
  EXPECT_EQ(video->topics[0].qos, "SENSOR_DATA");

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, IncludePostTriggerFalseIsParsed)
{
  auto dir = makeEmptyDir("forward");
  writeFile(
    dir / "incident.yaml",
    "topics:\n  - name: /camera/image_raw\n    include_post_trigger: false\n  - name: /odom\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  ASSERT_TRUE(result.ok);
  const auto * profile = result.profiles.find("incident");
  ASSERT_NE(profile, nullptr);
  ASSERT_EQ(profile->topics.size(), 2u);
  EXPECT_FALSE(profile->topics[0].include_post_trigger);
  EXPECT_TRUE(profile->topics[1].include_post_trigger);

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, DurationAndMemoryOverridesAreParsed)
{
  auto dir = makeEmptyDir("duration_memory");
  writeFile(
    dir / "incident.yaml",
    "topics:\n"
    "  - name: /camera/image_raw\n"
    "    duration_s: 45.0\n"
    "    memory_mb: 128.0\n"
    "  - name: /odom\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  ASSERT_TRUE(result.ok);
  const auto * profile = result.profiles.find("incident");
  ASSERT_NE(profile, nullptr);
  ASSERT_EQ(profile->topics.size(), 2u);
  ASSERT_TRUE(profile->topics[0].duration_s.has_value());
  EXPECT_DOUBLE_EQ(*profile->topics[0].duration_s, 45.0);
  ASSERT_TRUE(profile->topics[0].memory_mb.has_value());
  EXPECT_DOUBLE_EQ(*profile->topics[0].memory_mb, 128.0);
  EXPECT_FALSE(profile->topics[1].duration_s.has_value());
  EXPECT_FALSE(profile->topics[1].memory_mb.has_value());

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, NonPositiveDurationOrMemoryIsRejected)
{
  auto dir = makeEmptyDir("duration_memory_invalid");
  writeFile(dir / "good.yaml", "topics:\n  - name: /ok\n");
  writeFile(dir / "bad_duration.yaml", "topics:\n  - name: /x\n    duration_s: -5.0\n");
  writeFile(dir / "bad_memory.yaml", "topics:\n  - name: /x\n    memory_mb: 0\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_TRUE(result.ok);
  EXPECT_NE(result.profiles.find("good"), nullptr);
  EXPECT_EQ(result.profiles.find("bad_duration"), nullptr);
  EXPECT_EQ(result.profiles.find("bad_memory"), nullptr);
  EXPECT_EQ(result.warnings.size(), 2u);

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, SkipsMalformedFileButKeepsOthers)
{
  auto dir = makeEmptyDir("malformed");
  writeFile(dir / "good.yaml", "topics:\n  - name: /ok\n");
  writeFile(dir / "bad.yaml", "not_topics: true\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_TRUE(result.ok);
  EXPECT_EQ(result.profiles.profiles.size(), 1u);
  EXPECT_NE(result.profiles.find("good"), nullptr);
  EXPECT_EQ(result.profiles.find("bad"), nullptr);
  EXPECT_EQ(result.warnings.size(), 1u);

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, EmptyDirPathIsANoop)
{
  auto result = rosbag2_snapshot::loadProfilesDir("");
  EXPECT_TRUE(result.ok);
  EXPECT_TRUE(result.profiles.profiles.empty());
  EXPECT_TRUE(result.warnings.empty());
}

TEST(CaptureProfiles, MissingDirectoryReportsWarning)
{
  auto result = rosbag2_snapshot::loadProfilesDir("/nonexistent/path/should/not/exist");
  EXPECT_FALSE(result.ok);
  EXPECT_FALSE(result.warnings.empty());
}

TEST(CaptureProfiles, IncludeMergesTopicsAndOwnTopicsOverride)
{
  auto dir = makeEmptyDir("include_merge");
  writeFile(dir / "sensors.yaml", "topics:\n  - name: /imu\n    max_rate_hz: 10.0\n  - name: /odom\n");
  writeFile(dir / "video.yaml", "topics:\n  - name: /camera/image_raw\n    max_rate_hz: 2.0\n");
  writeFile(
    dir / "combo.yaml",
    "include: [sensors, video]\n"
    "topics:\n"
    "  - name: /odom\n"           // overrides sensors' /odom (0.0 -> 5.0)
    "    max_rate_hz: 5.0\n"
    "  - name: /extra\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_TRUE(result.warnings.empty());
  const auto * combo = result.profiles.find("combo");
  ASSERT_NE(combo, nullptr);
  ASSERT_EQ(combo->topics.size(), 4u);

  auto find_topic = [combo](const std::string & name) {
      return std::find_if(
        combo->topics.begin(), combo->topics.end(),
        [&name](const auto & t) {return t.name == name;});
    };

  auto imu = find_topic("/imu");
  ASSERT_NE(imu, combo->topics.end());
  EXPECT_DOUBLE_EQ(imu->max_rate_hz, 10.0);

  auto odom = find_topic("/odom");
  ASSERT_NE(odom, combo->topics.end());
  EXPECT_DOUBLE_EQ(odom->max_rate_hz, 5.0);  // combo's own entry won, not sensors'

  auto image = find_topic("/camera/image_raw");
  ASSERT_NE(image, combo->topics.end());
  EXPECT_DOUBLE_EQ(image->max_rate_hz, 2.0);

  EXPECT_NE(find_topic("/extra"), combo->topics.end());

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, IncludeOnlyNeedsNoOwnTopics)
{
  auto dir = makeEmptyDir("include_only");
  writeFile(dir / "sensors.yaml", "topics:\n  - name: /imu\n");
  writeFile(dir / "video.yaml", "topics:\n  - name: /camera/image_raw\n");
  writeFile(dir / "combo.yaml", "include: [sensors, video]\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_TRUE(result.warnings.empty());
  const auto * combo = result.profiles.find("combo");
  ASSERT_NE(combo, nullptr);
  EXPECT_EQ(combo->topics.size(), 2u);

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, UnknownIncludeDropsOnlyThatProfile)
{
  auto dir = makeEmptyDir("include_unknown");
  writeFile(dir / "good.yaml", "topics:\n  - name: /ok\n");
  writeFile(dir / "combo.yaml", "include: does_not_exist\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_NE(result.profiles.find("good"), nullptr);
  EXPECT_EQ(result.profiles.find("combo"), nullptr);
  EXPECT_FALSE(result.warnings.empty());

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, IncludeCycleDropsBothProfiles)
{
  auto dir = makeEmptyDir("include_cycle");
  writeFile(dir / "a.yaml", "include: b\ntopics:\n  - name: /a_topic\n");
  writeFile(dir / "b.yaml", "include: a\ntopics:\n  - name: /b_topic\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_EQ(result.profiles.find("a"), nullptr);
  EXPECT_EQ(result.profiles.find("b"), nullptr);
  EXPECT_FALSE(result.warnings.empty());

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, WriteKnobsAreParsed)
{
  auto dir = makeEmptyDir("write_knobs");
  writeFile(
    dir / "knobs.yaml",
    "topics:\n"
    "  - name: /cam\n    compression: jpg\n    compression_quality: 69\n"
    "  - name: /mask\n    compression: png\n"
    "  - name: /front\n    compression: h264\n    h264_throttle_skip: true\n"
    "  - name: /raw\n    compression: none\n"
    "  - name: /tf_static\n    duration_s: -1\n    override_old_timestamps: true\n"
    "  - name: /state\n    old_messages_to_keep: 1\n    queue_depth: 1\n"
    "  - name: /plain\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  ASSERT_TRUE(result.warnings.empty());
  const auto * p = result.profiles.find("knobs");
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->topics.size(), 7u);
  EXPECT_EQ(p->topics[0].compression, "jpg");
  EXPECT_EQ(p->topics[0].compression_quality.value_or(-1), 69);
  EXPECT_EQ(p->topics[1].compression, "png");
  EXPECT_FALSE(p->topics[1].compression_quality.has_value());
  EXPECT_EQ(p->topics[2].compression, "h264");
  EXPECT_TRUE(p->topics[2].h264_throttle_skip.value_or(false));
  EXPECT_EQ(p->topics[3].compression, "none");
  EXPECT_DOUBLE_EQ(p->topics[4].duration_s.value_or(0.0), -1.0);
  EXPECT_TRUE(p->topics[4].override_old_timestamps.value_or(false));
  EXPECT_EQ(p->topics[5].old_messages_to_keep.value_or(-1), 1);
  EXPECT_EQ(p->topics[5].queue_depth.value_or(-1), 1);
  EXPECT_TRUE(p->topics[6].compression.empty());
  EXPECT_FALSE(p->topics[6].override_old_timestamps.has_value());
  EXPECT_FALSE(p->topics[6].queue_depth.has_value());

  std::filesystem::remove_all(dir);
}

TEST(CaptureProfiles, InvalidWriteKnobsAreRejected)
{
  auto dir = makeEmptyDir("write_knobs_invalid");
  writeFile(dir / "good.yaml", "topics:\n  - name: /ok\n");
  writeFile(dir / "bad_format.yaml", "topics:\n  - name: /x\n    compression: webp\n");
  writeFile(
    dir / "bad_jpg_quality.yaml",
    "topics:\n  - name: /x\n    compression: jpg\n    compression_quality: 101\n");
  writeFile(
    dir / "bad_png_level.yaml",
    "topics:\n  - name: /x\n    compression: png\n    compression_quality: 10\n");
  writeFile(
    dir / "quality_without_format.yaml",
    "topics:\n  - name: /x\n    compression: h264\n    compression_quality: 5\n");
  writeFile(dir / "bad_queue_depth.yaml", "topics:\n  - name: /x\n    queue_depth: 0\n");
  writeFile(dir / "bad_old_messages.yaml", "topics:\n  - name: /x\n    old_messages_to_keep: -1\n");

  auto result = rosbag2_snapshot::loadProfilesDir(dir.string());

  EXPECT_NE(result.profiles.find("good"), nullptr);
  EXPECT_EQ(result.profiles.profiles.size(), 1u);
  EXPECT_EQ(result.warnings.size(), 6u);

  std::filesystem::remove_all(dir);
}
