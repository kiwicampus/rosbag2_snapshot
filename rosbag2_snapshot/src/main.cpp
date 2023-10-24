/* 
    Kiwi added this file
    Any inquires, please contact AI&Robotics Team, Kiwibot
*/

#include <memory>
#include "rclcpp/rclcpp.hpp"

#include <rosbag2_snapshot/snapshotter.hpp>

int main(int argc, char** argv)
{
    rclcpp::init(argc, argv);

    rclcpp::executors::SingleThreadedExecutor exec;
    auto options = rclcpp::NodeOptions().use_intra_process_comms(true);

    auto node_snapshotter = std::make_shared<rosbag2_snapshot::Snapshotter>(options);
    exec.add_node(node_snapshotter);
    exec.spin();
    rclcpp::shutdown();

    return 0;
}
