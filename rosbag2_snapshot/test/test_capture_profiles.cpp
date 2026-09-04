/*
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

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
