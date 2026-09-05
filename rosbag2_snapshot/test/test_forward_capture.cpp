#include "rosbag2_snapshot/forward_capture.hpp"

#include <gtest/gtest.h>

TEST(ForwardCapture, ZeroPostDurationIsNotAForwardRequest)
{
  EXPECT_FALSE(rosbag2_snapshot::isForwardCaptureRequest(0.0));
}

TEST(ForwardCapture, NegativePostDurationIsNotAForwardRequest)
{
  EXPECT_FALSE(rosbag2_snapshot::isForwardCaptureRequest(-1.0));
}

TEST(ForwardCapture, PositivePostDurationIsAForwardRequest)
{
  EXPECT_TRUE(rosbag2_snapshot::isForwardCaptureRequest(5.0));
}

TEST(ForwardCapture, ZeroMaxPostDurationDisablesForwardCapturesRegardlessOfRequest)
{
  EXPECT_FALSE(rosbag2_snapshot::forwardCaptureWithinLimit(5.0, 0.0));
  EXPECT_FALSE(rosbag2_snapshot::forwardCaptureWithinLimit(0.1, 0.0));
}

TEST(ForwardCapture, NegativeMaxPostDurationDisablesForwardCaptures)
{
  EXPECT_FALSE(rosbag2_snapshot::forwardCaptureWithinLimit(5.0, -1.0));
}

TEST(ForwardCapture, RequestWithinLimitIsAllowed)
{
  EXPECT_TRUE(rosbag2_snapshot::forwardCaptureWithinLimit(5.0, 300.0));
}

TEST(ForwardCapture, RequestExactlyAtLimitIsAllowed)
{
  EXPECT_TRUE(rosbag2_snapshot::forwardCaptureWithinLimit(300.0, 300.0));
}

TEST(ForwardCapture, RequestOverLimitIsRejected)
{
  EXPECT_FALSE(rosbag2_snapshot::forwardCaptureWithinLimit(300.1, 300.0));
}

TEST(ForwardCapture, NonForwardRequestIsNeverWithinLimitEvenIfEnabled)
{
  EXPECT_FALSE(rosbag2_snapshot::forwardCaptureWithinLimit(0.0, 300.0));
}
