/*
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

#include "rosbag2_snapshot/accepted_filename.hpp"

#include <gtest/gtest.h>

TEST(AcceptedFilename, EmptyIsRejected)
{
  EXPECT_FALSE(rosbag2_snapshot::hasAcceptedGoalFilename(""));
}

TEST(AcceptedFilename, DotBagSuffixIsAccepted)
{
  EXPECT_TRUE(rosbag2_snapshot::hasAcceptedGoalFilename("capture.bag"));
}

TEST(AcceptedFilename, DotMcapSuffixIsAccepted)
{
  EXPECT_TRUE(rosbag2_snapshot::hasAcceptedGoalFilename("capture.mcap"));
}

TEST(AcceptedFilename, PathWithDirectoriesIsAccepted)
{
  EXPECT_TRUE(rosbag2_snapshot::hasAcceptedGoalFilename("/data/2026/09/fault/abc123.mcap"));
}

TEST(AcceptedFilename, PrefixWithNoRecognizedSuffixIsRejected)
{
  EXPECT_FALSE(rosbag2_snapshot::hasAcceptedGoalFilename("capture"));
}

TEST(AcceptedFilename, SuffixMustBeAtTheEnd)
{
  EXPECT_FALSE(rosbag2_snapshot::hasAcceptedGoalFilename("capture.bag.old"));
  EXPECT_FALSE(rosbag2_snapshot::hasAcceptedGoalFilename("capture.mcap.old"));
}

TEST(AcceptedFilename, UnrelatedSuffixIsRejected)
{
  EXPECT_FALSE(rosbag2_snapshot::hasAcceptedGoalFilename("capture.txt"));
}
