/*
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

#include "rosbag2_snapshot/topic_resolver.hpp"

#include <rosbag2_transport/qos.hpp>

namespace rosbag2_snapshot
{

TopicResolver::TopicResolver(rclcpp::Node * node)
: node_(node)
{
}

bool TopicResolver::resolveType(const std::string & topic_name, std::string & type_out) const
{
  auto graph = node_->get_topic_names_and_types();
  auto it = graph.find(topic_name);
  if (it == graph.end() || it->second.empty()) {
    return false;
  }

  if (it->second.size() > 1) {
    RCLCPP_WARN(
      node_->get_logger(), "topic %s offers %zu types, recording as %s",
      topic_name.c_str(), it->second.size(), it->second.front().c_str());
  }

  type_out = it->second.front();
  return true;
}

bool TopicResolver::resolveQos(const std::string & topic_name, rclcpp::QoS & qos_out) const
{
  auto endpoints = node_->get_publishers_info_by_topic(topic_name);
  if (endpoints.empty()) {
    return false;
  }

  qos_out = rosbag2_transport::Rosbag2QoS::adapt_request_to_offers(topic_name, endpoints);
  return true;
}

}  // namespace rosbag2_snapshot
