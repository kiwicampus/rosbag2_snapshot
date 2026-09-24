#include "rosbag2_snapshot/profile_topic_details.hpp"

#include <gtest/gtest.h>

using rosbag2_snapshot::ProfileTopicSpec;
using rosbag2_snapshot::compressesTopicType;
using rosbag2_snapshot::profileTopicDetails;

TEST(ProfileTopicDetails, UnsetOptionsKeepTheTopicsOwn)
{
  ProfileTopicSpec spec{};
  spec.name = "/plain";
  const auto msg = profileTopicDetails(spec);
  EXPECT_EQ(msg.name, "/plain");
  EXPECT_FLOAT_EQ(msg.throttle_period, -1.0f);
  EXPECT_EQ(msg.use_compression, -1);
  EXPECT_EQ(msg.format, "");
  EXPECT_EQ(msg.jpg_quality, -1);
  EXPECT_EQ(msg.png_compression, -1);
  EXPECT_EQ(msg.override_old_timestamps, -1);
  EXPECT_EQ(msg.queue_depth, -1);
  EXPECT_EQ(msg.old_messages_to_keep, -1);
  EXPECT_EQ(msg.h264_throttle_skip, -1);
  EXPECT_EQ(msg.include_post_trigger, 1);
}

TEST(ProfileTopicDetails, CompressionDefaultsAndOverrides)
{
  ProfileTopicSpec jpg{};
  jpg.compression = "jpg";
  EXPECT_EQ(profileTopicDetails(jpg).jpg_quality, 95);
  jpg.compression_quality = 69;
  const auto with_quality = profileTopicDetails(jpg);
  EXPECT_EQ(with_quality.use_compression, 1);
  EXPECT_EQ(with_quality.format, "jpg");
  EXPECT_EQ(with_quality.jpg_quality, 69);
  EXPECT_EQ(with_quality.png_compression, -1);

  ProfileTopicSpec png{};
  png.compression = "png";
  EXPECT_EQ(profileTopicDetails(png).png_compression, 3);
  EXPECT_EQ(profileTopicDetails(png).jpg_quality, -1);

  ProfileTopicSpec h264{};
  h264.compression = "h264";
  EXPECT_EQ(profileTopicDetails(h264).format, "h264");
  EXPECT_EQ(profileTopicDetails(h264).jpg_quality, -1);

  ProfileTopicSpec none{};
  none.compression = "none";
  EXPECT_EQ(profileTopicDetails(none).use_compression, 0);
}

TEST(ProfileTopicDetails, WriteKnobsAndRate)
{
  ProfileTopicSpec spec{};
  spec.max_rate_hz = 4.0;
  spec.include_post_trigger = false;
  spec.override_old_timestamps = true;
  spec.queue_depth = 1;
  spec.old_messages_to_keep = 2;
  spec.h264_throttle_skip = false;
  const auto msg = profileTopicDetails(spec);
  EXPECT_FLOAT_EQ(msg.throttle_period, 0.25f);
  EXPECT_EQ(msg.include_post_trigger, 0);
  EXPECT_EQ(msg.override_old_timestamps, 1);
  EXPECT_EQ(msg.queue_depth, 1);
  EXPECT_EQ(msg.old_messages_to_keep, 2);
  EXPECT_EQ(msg.h264_throttle_skip, 0);
}

TEST(ProfileTopicDetails, OnlyImagesAreCompressed)
{
  EXPECT_TRUE(compressesTopicType(true, "sensor_msgs/msg/Image"));
  EXPECT_TRUE(compressesTopicType(true, ""));
  EXPECT_FALSE(compressesTopicType(true, "sensor_msgs/msg/PointCloud2"));
  EXPECT_FALSE(compressesTopicType(false, "sensor_msgs/msg/Image"));
}
