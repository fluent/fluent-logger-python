#   -*- coding: utf-8 -*-
from pybuilder.core import use_plugin, init, Author

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.flake8")
use_plugin("python.coverage")
use_plugin("python.coveralls")
use_plugin("python.distutils")
use_plugin("python.pycharm")
use_plugin("copy_resources")


name = "fluent-logger"
summary = "A Python logging handler for FluentD event collector"

authors = [Author("Kazuki Ohta", "kazuki.ohta@gmail.com")]
maintainers = [Author("Arcadiy Ivanov", "arcadiy@ivanov.biz")]

url = "https://github.com/fluent/fluent-logger-python"
urls = {"Bug Tracker": "https://github.com/fluent/fluent-logger-python/issues",
        "Source Code": "https://github.com/fluent/fluent-logger-python",
        "Documentation": "https://github.com/fluent/fluent-logger-python"
        }
license = "Apache License, Version 2.0"
version = "1.0.0.dev"

requires_python = ">=3.6"

default_task = ["analyze", "publish"]


@init
def set_properties(project):
    project.build_depends_on("docker", ">=5.0")
    project.build_depends_on("cryptography", ">=2.9.0")

    project.set_property("verbose", True)

    project.set_property("coverage_break_build", False)
    project.get_property("coverage_exceptions").extend(["setup"])

    project.set_property("flake8_break_build", True)
    project.set_property("flake8_extend_ignore", "E303")
    project.set_property("flake8_include_test_sources", True)
    project.set_property("flake8_max_line_length", 130)

    project.set_property("frosted_include_test_sources", True)
    project.set_property("frosted_include_scripts", True)

    project.set_property("copy_resources_target", "$dir_dist/fluent")
    project.get_property("copy_resources_glob").append("LICENSE")
    project.include_file("fluent", "LICENSE")

    # PyPy distutils needs os.environ['PATH'] not matter what
    # Also Windows needs PATH for DLL loading in all Pythons
    project.set_property("integrationtest_inherit_environment", True)

    project.set_property("distutils_readme_description", True)
    project.set_property("distutils_description_overwrite", True)
    project.set_property("distutils_readme_file", "README.rst")
    project.set_property("distutils_upload_skip_existing", True)
    project.set_property("distutils_setup_keywords", ["fluentd", "logging", "logger", "python"])

    project.set_property("distutils_classifiers", [
        "Programming Language :: Python",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only"
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Logging"
        "Intended Audience :: Developers",
        "Development Status :: 5 - Production/Stable",
])

