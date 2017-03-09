from setuptools import setup, Command
from distutils.sysconfig import get_python_lib
from glob import glob

import codecs
import os.path
import os
import subprocess
import sys

curdir = os.getcwd()
sys.path.append(curdir)

PACKAGE = "fleure-db"
VERSION = "0.1.0"
SNAPSHOT_BUILD_MODE = False

# For daily snapshot versioning mode:
if os.environ.get("_SNAPSHOT_BUILD", None) is not None:
    import datetime
    SNAPSHOT_BUILD_MODE = True
    VERSION = VERSION + datetime.datetime.now().strftime(".%Y%m%d")


def list_files(tdir):
    return [f for f in glob(os.path.join(tdir, '*')) if os.path.isfile(f)]


data_files = [
    ("/etc/fleure/db.d", list_files("data/conf/")),
    ("/usr/lib/systemd/system/", list_files("data/systemd/")),
]


class SrpmCommand(Command):

    user_options = []
    build_stage = "s"
    curdir = os.path.abspath(os.curdir)
    rpmspec = os.path.join(curdir, "pkg/package.spec")

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def update_mo(self):
        # os.system("./pkg/update-po.sh")  # TBD
        pass

    def pre_sdist(self):
        if not SNAPSHOT_BUILD_MODE:
            self.update_mo()

        c = open(self.rpmspec + ".in").read()
        open(self.rpmspec, "w").write(c.replace("@VERSION@", VERSION))

    def run(self):
        self.pre_sdist()
        self.run_command('sdist')
        self.build_rpm()

    def build_rpm(self):
        rpmbuild = os.path.join(self.curdir, "pkg/rpmbuild-wrapper.sh")
        workdir = os.path.join(self.curdir, "dist")
        cmd_s = "%s -w %s -s %s %s" % (rpmbuild, workdir, self.build_stage,
                                       self.rpmspec)
        subprocess.check_call(cmd_s, shell=True)


class RpmCommand(SrpmCommand):

    build_stage = "b"


setup(name=PACKAGE,
      version=VERSION,
      description="An utlity to analyze updateinfo",
      long_description=codecs.open("README.rst", 'r', 'utf-8').read(),
      author="Satoru SATOH",
      author_email="ssato@redhat.com",
      license="AGPLv3+, GPLv3+ and MIT",
      url="https://github.com/ssato/fleure-db",
      packages=["fleure_db"],
      include_package_data=True,
      entry_points=open(os.path.join(os.curdir,
                        "pkg/entry_points.txt")).read(),
      cmdclass=dict(srpm=SrpmCommand, rpm=RpmCommand),
      data_files=data_files)

# vim:sw=4:ts=4:et:
