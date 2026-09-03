/*
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

#include "rosbag2_snapshot/staging_path.hpp"

#include <gtest/gtest.h>

#include <filesystem>

TEST(StagingPath, AppendsATmpSuffix)
{
  EXPECT_EQ(
    rosbag2_snapshot::stagingPathFor("/tmp/foo.bag").string(),
    "/tmp/foo.bag.tmp");
}

TEST(StagingPath, StaysInTheSameDirectoryAsTheFinalPath)
{
  std::filesystem::path final_path = "/tmp/some/nested/dir/event.bag";
  std::filesystem::path staging_path = rosbag2_snapshot::stagingPathFor(final_path);

  // This is what guarantees the rename in finalizeCapture() is an atomic,
  // same-filesystem rename rather than a fallback copy.
  EXPECT_EQ(staging_path.parent_path(), final_path.parent_path());
}

TEST(StagingPath, IsStableForTheSameInput)
{
  std::filesystem::path final_path = "/tmp/foo.bag";
  EXPECT_EQ(rosbag2_snapshot::stagingPathFor(final_path), rosbag2_snapshot::stagingPathFor(final_path));
}
