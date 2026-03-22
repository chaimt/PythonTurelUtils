import importlib_metadata


def _get_distro_version(distribution: str) -> str:
    """
    Get the version of an installed distribution
    """
    try:
        version = importlib_metadata.version(distribution)
    except importlib_metadata.PackageNotFoundError:
        version = "0.0"
    return version
