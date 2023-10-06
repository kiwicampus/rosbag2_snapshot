cmake_minimum_required(VERSION 3.5)
project(rosbag2_snapshot)

# Default to C99
if(NOT CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 99)
endif()

# Default to C++20
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 20)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(rosbag2_compression REQUIRED)
find_package(rosbag2_compression_zstd REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(std_srvs REQUIRED)
find_package(rosbag2_snapshot_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(OpenCV REQUIRED)
find_package(cv_bridge REQUIRED)

include_directories(
  include
)

ament_export_include_directories(include)

set(DEPENDENCIES
  rosbag2_cpp
  rosbag2_compression
  rosbag2_compression_zstd
  rclcpp
  rclcpp_components
  std_srvs
  rosbag2_snapshot_msgs
  sensor_msgs
  OpenCV
  cv_bridge
)

set(library_name ${PROJECT_NAME}_core)

add_library(${library_name} SHARED
  include/rosbag2_snapshot/snapshotter.hpp
# src/snapshot.cpp
  src/snapshotter.cpp
)

ament_target_dependencies(${library_name}
  ${DEPENDENCIES}
)

# Declare rosbag2_snapshot node component
rclcpp_components_register_nodes(${library_name} "rosbag2_snapshot::Snapshotter")
rclcpp_components_register_nodes(${library_name} "rosbag2_snapshot::SnapshotterClient")

install(TARGETS ${library_name}
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

ament_package()
