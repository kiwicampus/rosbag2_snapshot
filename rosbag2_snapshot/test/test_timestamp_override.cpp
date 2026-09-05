#include "rosbag2_snapshot/timestamp_override.hpp"

#include <gtest/gtest.h>

TEST(TimestampOverride, NeitherFlagSetNeverOverrides)
{
  EXPECT_FALSE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    false, 0, true, true, 100, 0));
}

TEST(TimestampOverride, FlagSetButNoWindowSpecifiedNeverOverrides)
{
  // Regression case: start_time/stop_time both left at zero (every forward
  // capture, and any request that doesn't set them) must never trigger the
  // override, even though the message is far "older" than a zero-length
  // window would suggest.
  EXPECT_FALSE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    true, 0, false, false, 1'000'000'000, 0));
  EXPECT_FALSE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    false, 1, false, false, 1'000'000'000, 0));
}

TEST(TimestampOverride, OverrideOldTimestampsFlagWithWindowAndOldMessageOverrides)
{
  EXPECT_TRUE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    true, 0, true, true, 100, 50));
}

TEST(TimestampOverride, OldMessagesToKeepAloneWithWindowAndOldMessageOverrides)
{
  EXPECT_TRUE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    false, 1, true, true, 100, 50));
}

TEST(TimestampOverride, StartTimeAloneCountsAsAWindow)
{
  EXPECT_TRUE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    true, 0, true, false, 100, 50));
}

TEST(TimestampOverride, StopTimeAloneCountsAsAWindow)
{
  EXPECT_TRUE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    true, 0, false, true, 100, 50));
}

TEST(TimestampOverride, MessageWithinTheWindowIsNotOverridden)
{
  EXPECT_FALSE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    true, 0, true, true, 30, 50));
}

TEST(TimestampOverride, MessageExactlyAtTheWindowBoundaryIsNotOverridden)
{
  EXPECT_FALSE(rosbag2_snapshot::shouldOverrideOldTimestamp(
    true, 0, true, true, 50, 50));
}
