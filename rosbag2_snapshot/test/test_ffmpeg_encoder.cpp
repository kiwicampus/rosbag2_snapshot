#include <gtest/gtest.h>

#include <memory>

#include <opencv2/core.hpp>
#include <rclcpp/rclcpp.hpp>

#include "rosbag2_snapshot/ffmpeg_encoding/ffmpeg_encoder.hpp"

using ffmpeg_image_transport::FFMPEGEncoder;

namespace
{

class FFMPEGEncoderTest : public ::testing::Test
{
protected:
  static void SetUpTestSuite() { rclcpp::init(0, nullptr); }
  static void TearDownTestSuite() { rclcpp::shutdown(); }
};

cv::Mat noiseFrame(int width, int height)
{
  cv::Mat img(height, width, CV_8UC3);
  cv::randu(img, cv::Scalar::all(0), cv::Scalar::all(255));
  return img;
}

}  // namespace

// Default settings must emit a packet per frame: libx264's own defaults hold
// back tens of frames, which left short captures with nothing but empty messages.
TEST_F(FFMPEGEncoderTest, DefaultsEmitAPacketPerFrame)
{
  auto node = std::make_shared<rclcpp::Node>("test_ffmpeg_encoder");
  FFMPEGEncoder encoder;
  encoder.setParameters(node.get(), "h264.");
  ASSERT_TRUE(encoder.initialize(640, 480));

  for (int i = 0; i < 20; ++i) {
    std_msgs::msg::Header header;
    header.stamp = rclcpp::Time(int64_t{1000} + i, RCL_ROS_TIME);
    header.frame_id = "cam";
    encoder.encodeImage(noiseFrame(640, 480), header, rclcpp::Time(int64_t{0}, RCL_ROS_TIME));

    foxglove_msgs::msg::CompressedVideo out;
    ASSERT_TRUE(encoder.takeCompressedImage(out)) << "no packet for frame " << i;
    EXPECT_EQ(out.format, "h264");
    EXPECT_EQ(out.frame_id, "cam");
    EXPECT_FALSE(out.data.empty());
  }
}

TEST_F(FFMPEGEncoderTest, TakeClearsThePacket)
{
  auto node = std::make_shared<rclcpp::Node>("test_ffmpeg_encoder_take");
  FFMPEGEncoder encoder;
  encoder.setParameters(node.get(), "h264.");
  ASSERT_TRUE(encoder.initialize(640, 480));

  foxglove_msgs::msg::CompressedVideo out;
  EXPECT_FALSE(encoder.takeCompressedImage(out));

  encoder.encodeImage(noiseFrame(640, 480), std_msgs::msg::Header(), rclcpp::Time(int64_t{0}, RCL_ROS_TIME));
  EXPECT_TRUE(encoder.takeCompressedImage(out));
  EXPECT_FALSE(encoder.takeCompressedImage(out));
}

TEST_F(FFMPEGEncoderTest, FFmpegLogsOnlyAtDebug)
{
  auto node = std::make_shared<rclcpp::Node>("test_ffmpeg_encoder_log");
  FFMPEGEncoder encoder;

  node->get_logger().set_level(rclcpp::Logger::Level::Info);
  encoder.setParameters(node.get(), "h264.");
  EXPECT_EQ(av_log_get_level(), AV_LOG_WARNING);

  node->get_logger().set_level(rclcpp::Logger::Level::Debug);
  encoder.setParameters(node.get(), "h264.");
  EXPECT_EQ(av_log_get_level(), AV_LOG_INFO);
}
