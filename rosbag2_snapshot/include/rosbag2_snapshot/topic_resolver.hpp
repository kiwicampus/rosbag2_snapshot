/*
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

#ifndef ROSBAG2_SNAPSHOT__TOPIC_RESOLVER_HPP_
#define ROSBAG2_SNAPSHOT__TOPIC_RESOLVER_HPP_

#include <rclcpp/rclcpp.hpp>

#include <string>

namespace rosbag2_snapshot
{

// Resolves a topic's type and QoS from the running ROS graph, the way
// `ros2 bag record` does, instead of requiring both to be hand-declared.
// A topic whose publisher has not appeared yet cannot be resolved; the
// caller is expected to retry later (Snapshotter does this on its existing
// poll_topic_timer_).
class TopicResolver
{
public:
  explicit TopicResolver(rclcpp::Node * node);

  // Message type from the graph. False if the topic has no publisher (or
  // subscriber) yet. If publishers disagree on the type, the first one is
  // used and a warning is logged rather than picked silently.
  bool resolveType(const std::string & topic_name, std::string & type_out) const;

  // QoS adapted to what the topic's publishers currently offer (most
  // permissive wins on disagreement) via
  // rosbag2_transport::Rosbag2QoS::adapt_request_to_offers -- the same rule
  // `ros2 bag record` uses, so a BEST_EFFORT publisher (a camera, typically)
  // is actually matched instead of silently buffering nothing. False if
  // there is no publisher yet.
  bool resolveQos(const std::string & topic_name, rclcpp::QoS & qos_out) const;

private:
  rclcpp::Node * node_;
};

}  // namespace rosbag2_snapshot

#endif  // ROSBAG2_SNAPSHOT__TOPIC_RESOLVER_HPP_
