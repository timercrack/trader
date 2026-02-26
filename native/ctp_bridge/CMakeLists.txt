cmake_minimum_required(VERSION 3.18)
project(ctp_bridge_native LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

execute_process(
    COMMAND "${Python3_EXECUTABLE}" -m pybind11 --cmakedir
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(ctp_bridge_native src/py_module.cpp)

if(MSVC)
    target_compile_options(ctp_bridge_native PRIVATE /std:c++latest /utf-8)
endif()

if(WIN32)
    set(THOST_API_DIR ${CMAKE_CURRENT_SOURCE_DIR}/api/win)
elseif(UNIX)
    set(THOST_API_DIR ${CMAKE_CURRENT_SOURCE_DIR}/api/linux)
else()
    message(FATAL_ERROR "Unsupported platform for Thost API")
endif()

target_include_directories(ctp_bridge_native PRIVATE
    ${THOST_API_DIR}
)

if(WIN32)
    set(THOST_TRADER_LIB ${THOST_API_DIR}/thosttraderapi_se.lib)
    set(THOST_MD_LIB ${THOST_API_DIR}/thostmduserapi_se.lib)
    if(EXISTS ${THOST_TRADER_LIB} AND EXISTS ${THOST_MD_LIB})
        target_link_libraries(ctp_bridge_native PRIVATE
            ${THOST_TRADER_LIB}
            ${THOST_MD_LIB}
        )
    else()
        message(WARNING "Thost .lib files not found under ${THOST_API_DIR}, build will use header-only skeleton.")
    endif()
endif()

if(UNIX)
    set(THOST_TRADER_SO ${THOST_API_DIR}/thosttraderapi_se.so)
    set(THOST_MD_SO ${THOST_API_DIR}/thostmduserapi_se.so)
    if(EXISTS ${THOST_TRADER_SO} AND EXISTS ${THOST_MD_SO})
        target_link_directories(ctp_bridge_native PRIVATE ${THOST_API_DIR})
        target_link_libraries(ctp_bridge_native PRIVATE
            "-l:thosttraderapi_se.so"
            "-l:thostmduserapi_se.so"
        )
        set_target_properties(ctp_bridge_native PROPERTIES
            BUILD_RPATH "${THOST_API_DIR}"
            INSTALL_RPATH "$ORIGIN/native/ctp_bridge/api/linux;$ORIGIN"
        )
    else()
        message(WARNING "Thost .so files not found under ${THOST_API_DIR}, build will use header-only skeleton.")
    endif()
endif()
