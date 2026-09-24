#include <gtest/gtest.h>

#include <iterator>
#include <memory>

#include "rosbag2_snapshot/snapshotter.hpp"

using rosbag2_snapshot::MessageQueue;
using rosbag2_snapshot::SnapshotMessage;
using rosbag2_snapshot::SnapshotterTopicOptions;

namespace
{

constexpr int64_t kSecond = 1000000000LL;

MessageQueue makeRing(double duration_s)
{
  return MessageQueue(
    SnapshotterTopicOptions(
      rclcpp::Duration::from_seconds(duration_s), SnapshotterTopicOptions::NO_MEMORY_LIMIT),
    rclcpp::get_logger("test_forward_follow"));
}

SnapshotMessage messageAt(int64_t ns)
{
  auto msg = std::make_shared<rclcpp::SerializedMessage>(16);
  msg->get_rcl_serialized_message().buffer_length = 16;
  return SnapshotMessage(msg, rclcpp::Time(ns, RCL_ROS_TIME));
}

size_t countOf(MessageQueue & queue)
{
  auto range = queue.rangeFromTimes(rclcpp::Time(int64_t{0}, RCL_ROS_TIME), rclcpp::Time(int64_t{0}, RCL_ROS_TIME));
  return static_cast<size_t>(std::distance(range.first, range.second));
}

}  // namespace

TEST(ForwardFollow, FollowerOutlivesTheRingWindow)
{
  auto ring = makeRing(1.0);
  for (int64_t t = 1; t <= 10; ++t) {
    ring.push(messageAt(t * kSecond / 10));
  }
  auto follower = ring.cloneAndFollow();
  const size_t pre_trigger = countOf(*follower);
  for (int64_t t = 11; t <= 50; ++t) {
    ring.push(messageAt(t * kSecond / 10));
  }

  EXPECT_EQ(countOf(*follower), pre_trigger + 40);
  EXPECT_LT(countOf(ring), countOf(*follower));
  auto range = follower->rangeFromTimes(rclcpp::Time(int64_t{0}, RCL_ROS_TIME), rclcpp::Time(int64_t{0}, RCL_ROS_TIME));
  EXPECT_EQ(range.first->time.nanoseconds(), kSecond / 10);
  EXPECT_EQ(std::prev(range.second)->time.nanoseconds(), 5 * kSecond);
}

TEST(ForwardFollow, UnfollowStopsDelivery)
{
  auto ring = makeRing(10.0);
  ring.push(messageAt(kSecond));
  auto follower = ring.cloneAndFollow();
  ring.push(messageAt(2 * kSecond));
  ring.unfollow(follower);
  ring.push(messageAt(3 * kSecond));

  EXPECT_EQ(countOf(*follower), 2u);
  EXPECT_EQ(countOf(ring), 3u);
}

TEST(ForwardFollow, PlainCloneDoesNotFollow)
{
  auto ring = makeRing(10.0);
  ring.push(messageAt(kSecond));
  auto copy = ring.clone();
  ring.push(messageAt(2 * kSecond));

  EXPECT_EQ(countOf(*copy), 1u);
}
