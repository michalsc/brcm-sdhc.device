cmake_minimum_required(VERSION 3.14.0)
project(brcm-sdhc VERSION 1.8.0)

include(cmake/bin2h.cmake)
include(cmake/verstring.cmake)
get_verstring(VERSTRING)

if(NOT TARGET mailbox)
    add_subdirectory(mailbox.resource EXCLUDE_FROM_ALL)
endif()
if(NOT TARGET devicetree)
    add_subdirectory(devicetree.resource EXCLUDE_FROM_ALL)
endif()

add_executable(brcm-sdhc.device
    src/main.c
    src/expunge.c
    src/extfunc.c
    src/open.c
    src/close.c
    src/init.c
    src/beginio.c
    src/io.c
    src/abortio.c
    src/mbox.c
    src/emmc.c
    src/sdhost.c
    src/unit_task.c
    src/findtoken.c
    src/debug.c
)

set_source_files_properties(src/abortio.c PROPERTIES COMPILE_FLAGS 
    "-fcall-saved-d1 -fcall-saved-a0 -fcall-saved-a1")
set_source_files_properties(src/beginio.c PROPERTIES COMPILE_FLAGS 
    "-fcall-saved-d1 -fcall-saved-a0 -fcall-saved-a1")

bin_to_header(brcm-sdhc.device)

target_compile_options(brcm-sdhc.device PRIVATE -Os -m68040 -msmall-code -mregparm=4 -fomit-frame-pointer)
target_compile_definitions(brcm-sdhc.device PRIVATE 
    VERSION_STRING="${VERSTRING}"
    SDCARD_VERSION=${CMAKE_PROJECT_VERSION_MAJOR}
    SDCARD_REVISION=${CMAKE_PROJECT_VERSION_MINOR})
target_include_directories(brcm-sdhc.device PRIVATE include)
target_link_options(brcm-sdhc.device PRIVATE -ffreestanding -nostdlib -nostartfiles -s -Wl,-e_rom_start -Wl,-T${CMAKE_CURRENT_SOURCE_DIR}/ldscript.lds)
target_link_libraries(brcm-sdhc.device devicetree)

set_target_properties(brcm-sdhc.device PROPERTIES LINK_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ldscript.lds)
