#include <gtest/gtest.h>

#include "rosbag2_snapshot/snapshotter.hpp"

using rosbag2_snapshot::DetailsMsg;
using rosbag2_snapshot::ImageCompressionOptions;
using rosbag2_snapshot::applyFormatOverride;

namespace
{

ImageCompressionOptions pngTopic()
{
  ImageCompressionOptions opts;
  opts.use_compression = true;
  opts.format = "png";
  opts.imwrite_flag = cv::IMWRITE_PNG_COMPRESSION;
  opts.imwrite_flag_value = 3;
  return opts;
}

ImageCompressionOptions jpgTopic(int quality)
{
  ImageCompressionOptions opts;
  opts.use_compression = true;
  opts.format = "jpg";
  opts.imwrite_flag = cv::IMWRITE_JPEG_QUALITY;
  opts.imwrite_flag_value = quality;
  return opts;
}

DetailsMsg formatOverride(const std::string & format)
{
  DetailsMsg msg{};
  msg.format = format;
  return msg;
}

}  // namespace

TEST(CompressionOverride, H264FallbackOnAPngTopicUsesTheDefaultJpgQuality)
{
  auto opts = pngTopic();
  ASSERT_TRUE(applyFormatOverride(formatOverride("h264"), opts));
  EXPECT_TRUE(opts.h264);
  EXPECT_EQ(opts.format, "jpg");
  EXPECT_EQ(opts.imwrite_flag, cv::IMWRITE_JPEG_QUALITY);
  EXPECT_EQ(opts.imwrite_flag_value, 95);
}

TEST(CompressionOverride, H264FallbackOnAJpgTopicKeepsItsQuality)
{
  auto opts = jpgTopic(60);
  ASSERT_TRUE(applyFormatOverride(formatOverride("h264"), opts));
  EXPECT_EQ(opts.imwrite_flag, cv::IMWRITE_JPEG_QUALITY);
  EXPECT_EQ(opts.imwrite_flag_value, 60);
}

TEST(CompressionOverride, JpgWithoutQualityOnAPngTopicUsesTheDefault)
{
  auto opts = pngTopic();
  ASSERT_TRUE(applyFormatOverride(formatOverride("jpg"), opts));
  EXPECT_EQ(opts.imwrite_flag, cv::IMWRITE_JPEG_QUALITY);
  EXPECT_EQ(opts.imwrite_flag_value, 95);
}

TEST(CompressionOverride, PngWithoutLevelOnAJpgTopicUsesTheDefault)
{
  auto opts = jpgTopic(60);
  ASSERT_TRUE(applyFormatOverride(formatOverride("png"), opts));
  EXPECT_EQ(opts.imwrite_flag, cv::IMWRITE_PNG_COMPRESSION);
  EXPECT_EQ(opts.imwrite_flag_value, 3);
}

TEST(CompressionOverride, ExplicitValuesWin)
{
  auto opts = pngTopic();
  auto msg = formatOverride("jpg");
  msg.jpg_quality = 40;
  ASSERT_TRUE(applyFormatOverride(msg, opts));
  EXPECT_EQ(opts.imwrite_flag_value, 40);

  msg = formatOverride("png");
  msg.png_compression = 7;
  ASSERT_TRUE(applyFormatOverride(msg, opts));
  EXPECT_EQ(opts.imwrite_flag_value, 7);
}

TEST(CompressionOverride, UnknownFormatDisablesCompression)
{
  auto opts = jpgTopic(60);
  EXPECT_FALSE(applyFormatOverride(formatOverride("webp"), opts));
  EXPECT_FALSE(opts.use_compression);
}
