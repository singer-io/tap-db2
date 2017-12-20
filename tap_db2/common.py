import os
import re
import shutil
import pyodbc
import backoff

# pylint: disable=no-member

userprefs = """
[CWB_CURRUSER\Software\IBM\Client Access Express\CurrentVersion\Environments\My Connections\127.0.0.1\Communication]
Port lookup mode=attr_dwd:0x00000001
"""


def write_userprefs():
    userprefs_fname = os.path.expanduser("~/.iSeriesAccess/cwb_userprefs.ini")
    if not os.path.exists(userprefs_fname):
        os.makedirs(os.path.dirname(userprefs_fname), 0o700, exist_ok=True)
        with open(userprefs_fname, "w") as f:
            f.write(userprefs)


def write_services_port(port, backup_first=True):
    if backup_first:
        shutil.copyfile("/etc/services", "/etc/services.backup")
    with open("/etc/services", "r+") as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            if not re.match(r"^as-database\s+", line):
                f.write(line)
        f.truncate()
        f.write("as-database {}/tcp\n".format(port))


@backoff.on_exception(backoff.expo,
                      (pyodbc.OperationalError),
                      max_tries=5,
                      factor=2)
def connection(config):
    return pyodbc.connect(
        driver="{iSeries Access ODBC Driver 64-bit}",
        system=config["db2_system"],
        uid=config["db2_uid"],
        pwd=config["db2_pwd"])


class get_cursor(object):
    def __init__(self, config):
        self.conn = connection(config)
        self.cur = self.conn.cursor()

    def __enter__(self):
        return self.cur

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.cur.close()
        self.conn.close()
