#include "rosbag2_snapshot/shared_memory_budget.hpp"

#include <gtest/gtest.h>

using rosbag2_snapshot::SharedMemoryBudget;

TEST(SharedMemoryBudget, ZeroLimitIsUnlimited)
{
  SharedMemoryBudget budget;
  EXPECT_TRUE(budget.fits(1'000'000'000));
  budget.add(1'000'000'000);
  EXPECT_TRUE(budget.fits(1'000'000'000));
}

TEST(SharedMemoryBudget, NegativeLimitIsUnlimited)
{
  SharedMemoryBudget budget(-1);
  EXPECT_TRUE(budget.fits(1'000'000'000));
}

TEST(SharedMemoryBudget, FitsUnderLimit)
{
  SharedMemoryBudget budget(100);
  EXPECT_TRUE(budget.fits(100));
  EXPECT_FALSE(budget.fits(101));
}

TEST(SharedMemoryBudget, AddTracksUsage)
{
  SharedMemoryBudget budget(100);
  budget.add(60);
  EXPECT_EQ(budget.used(), 60);
  EXPECT_TRUE(budget.fits(40));
  EXPECT_FALSE(budget.fits(41));
}

TEST(SharedMemoryBudget, ReleaseFreesRoom)
{
  SharedMemoryBudget budget(100);
  budget.add(80);
  budget.release(50);
  EXPECT_EQ(budget.used(), 30);
  EXPECT_TRUE(budget.fits(70));
}

TEST(SharedMemoryBudget, ReleaseNeverGoesNegative)
{
  SharedMemoryBudget budget(100);
  budget.add(10);
  budget.release(50);
  EXPECT_EQ(budget.used(), 0);
}

TEST(SharedMemoryBudget, SetLimitChangesCapacity)
{
  SharedMemoryBudget budget;
  budget.setLimit(50);
  budget.add(40);
  EXPECT_TRUE(budget.fits(10));
  EXPECT_FALSE(budget.fits(11));
}
