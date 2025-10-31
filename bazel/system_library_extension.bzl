"""Module extension for importing system libraries."""

load("//bazel:system_library.bzl", "system_library_repository")

def _system_library_impl(module_ctx):
    """Implementation of system_library module extension."""

    # Register all system libraries requested by modules
    for mod in module_ctx.modules:
        for lib in mod.tags.library:
            system_library_repository(
                name = lib.name,
                libname = lib.libname,
                pkg_config_name = lib.pkg_config_name if hasattr(lib, "pkg_config_name") else None,
            )

# Define the tag class for library declarations
_library_tag = tag_class(
    attrs = {
        "name": attr.string(
            doc = "Name of the repository to create",
            mandatory = True,
        ),
        "libname": attr.string(
            doc = "The name of the library (without 'lib' prefix or extension)",
            mandatory = True,
        ),
        "pkg_config_name": attr.string(
            doc = "The pkg-config package name (defaults to libname if not specified)",
        ),
    },
)

# Define the module extension
system_library = module_extension(
    implementation = _system_library_impl,
    tag_classes = {
        "library": _library_tag,
    },
    environ = ["*"],  # Make extension sensitive to all environment variables
)
