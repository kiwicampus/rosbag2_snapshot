cmake_minimum_required(VERSION 3.5)
project(rosbag2_snapshot)

# Default to C99
if(NOT CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 99)
endif()

# Default to C++17
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
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
find_package(ffmpeg_image_transport_msgs REQUIRED)
find_package(foxglove_msgs REQUIRED)
find_package(visualization_msgs REQUIRED)

set(ENV{PKG_CONFIG_PATH} ${FFMPEG_PKGCONFIG})
find_package(PkgConfig REQUIRED)

pkg_check_modules(LIBAV REQUIRED IMPORTED_TARGET
    libavcodec
    libswresample
    libswscale
    libavutil
  )

include_directories(
  include
)

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
  ffmpeg_image_transport_msgs
  foxglove_msgs
  visualization_msgs
)

set(executable_name snapshotter)
set(library_name ${executable_name}_core)

add_library(${library_name} SHARED
  include/rosbag2_snapshot/snapshotter.hpp
# src/snapshot.cpp
  src/snapshotter.cpp
)

add_library(encoder SHARED
  src/ffmpeg_encoding/ffmpeg_encoder.cpp
  src/ffmpeg_encoding/tdiff.cpp
  src/ffmpeg_encoding/utils.cpp
)
ament_target_dependencies(encoder
  ${DEPENDENCIES}
)

add_executable(${executable_name}
  src/main.cpp
)

target_link_libraries( ${library_name}
  PkgConfig::LIBAV
  encoder
)

ament_target_dependencies(${library_name}
  ${DEPENDENCIES}
)

target_link_libraries(${executable_name}
  ${library_name}
  PkgConfig::LIBAV
  encoder
)
ament_target_dependencies(${executable_name}
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

install(TARGETS ${executable_name}
  DESTINATION lib/${PROJECT_NAME}
)

install(TARGETS encoder
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

ament_package()
