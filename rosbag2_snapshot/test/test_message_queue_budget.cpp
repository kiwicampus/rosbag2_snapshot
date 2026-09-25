#include <gtest/gtest.h>

#include <memory>

#include "rosbag2_snapshot/shared_memory_budget.hpp"
#include "rosbag2_snapshot/snapshotter.hpp"

using rosbag2_snapshot::MessageQueue;
using rosbag2_snapshot::SharedMemoryBudget;
using rosbag2_snapshot::SnapshotMessage;
using rosbag2_snapshot::SnapshotterTopicOptions;

namespace
{

constexpr int64_t kSecond = 1000000000LL;

SnapshotterTopicOptions ringOptions()
{
  return SnapshotterTopicOptions(
    rclcpp::Duration::from_seconds(10.0), SnapshotterTopicOptions::NO_MEMORY_LIMIT);
}

SnapshotMessage messageAt(int64_t ns)
{
  auto msg = std::make_shared<rclcpp::SerializedMessage>(16);
  msg->get_rcl_serialized_message().buffer_length = 16;
  return SnapshotMessage(msg, rclcpp::Time(ns, RCL_ROS_TIME));
}

}  // namespace

// The shared budget must always equal what the queues hold, or a leak
// accumulates until every push is refused.
TEST(MessageQueueBudget, ClearReleasesItsBytes)
{
  SharedMemoryBudget budget(1'000'000);
  MessageQueue queue(ringOptions(), rclcpp::get_logger("test"), &budget);
  for (int64_t t = 1; t <= 3; ++t) {
    queue.push(messageAt(t * kSecond));
  }
  ASSERT_GT(queue.usedBytes(), 0);
  ASSERT_EQ(budget.used(), queue.usedBytes());

  queue.clear();

  EXPECT_EQ(queue.usedBytes(), 0);
  EXPECT_EQ(budget.used(), 0);
}

TEST(MessageQueueBudget, TimeGoingBackwardsKeepsTheBudgetInStep)
{
  SharedMemoryBudget budget(1'000'000);
  MessageQueue queue(ringOptions(), rclcpp::get_logger("test"), &budget);
  queue.push(messageAt(5 * kSecond));
  queue.push(messageAt(6 * kSecond));

  // Older than the back: the queue starts over with this one message.
  queue.push(messageAt(1 * kSecond));

  EXPECT_EQ(budget.used(), queue.usedBytes());
  EXPECT_EQ(budget.used(), queue.getMessageSize(messageAt(1 * kSecond)));
}
