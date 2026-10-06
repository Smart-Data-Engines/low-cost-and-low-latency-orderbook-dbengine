# Run by CPack once the packages are built (CPACK_POST_BUILD_SCRIPTS): writes every .tar.gz again
# with each entry owned by 0/0 and nothing writable by its group or by others.
#
# CPack's archive generators record the owner of the staging files - the uid of whoever built them,
# `runner` (1001) on a CI runner - and no CMake up to 4.2 lets a project choose it. GNU tar run as
# root restores an entry's owner and mode, onto directories that already exist as well, so the
# tarball extracted over / as docs/operations.md said handed /etc, /usr and /usr/bin to that uid
# (#210). The archive's one top-level directory is kept: the documented `--strip-components=1`
# depends on it.
foreach(package IN LISTS CPACK_PACKAGE_FILES)
    if(NOT package MATCHES "\\.tar\\.gz$")
        continue()
    endif()
    set(unpacked "${package}.unpacked")
    file(REMOVE_RECURSE "${unpacked}")
    file(MAKE_DIRECTORY "${unpacked}")
    execute_process(COMMAND tar -xzf "${package}" -C "${unpacked}"
                    RESULT_VARIABLE rc ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "archive_owned_by_root: could not unpack ${package}: ${err}")
    endif()
    file(GLOB top RELATIVE "${unpacked}" "${unpacked}/*")
    list(LENGTH top count)
    if(NOT count EQUAL 1)
        message(FATAL_ERROR "archive_owned_by_root: ${package} holds ${count} top-level entries, not one: ${top}")
    endif()
    execute_process(COMMAND tar --create --gzip --file "${package}"
                            --owner=0 --group=0 --numeric-owner --mode=go-w --sort=name
                            -C "${unpacked}" "${top}"
                    RESULT_VARIABLE rc ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "archive_owned_by_root: could not write ${package} again: ${err}")
    endif()
    file(REMOVE_RECURSE "${unpacked}")
    message(STATUS "archive_owned_by_root: ${package}: every entry 0/0, none writable beyond its owner")
endforeach()
