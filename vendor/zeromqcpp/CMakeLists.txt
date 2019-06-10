# Resources:
#  - http://zeromq.org/intro:get-the-software
#  - http://bit.ly/2dK0UBT
#
# Remember to have libtool, pkg-config, build-essential, autoconf, and automake
# installed.

CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

INCLUDE(ExternalProject)

EXTERNALPROJECT_ADD(zeromqcpp
    GIT_REPOSITORY "https://github.com/zeromq/cppzmq.git"
    GIT_TAG "v4.2.3"
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

SET(ZEROMQCPP_INCLUDE_DIRS
    ${CMAKE_CURRENT_BINARY_DIR}/zeromqcpp-prefix/src/zeromqcpp PARENT_SCOPE)
