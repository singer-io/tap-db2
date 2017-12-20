import os
import re
import shutil
import configparser
import pyodbc
import backoff

# pylint: disable=no-member


def _write_userprefs(host, port):
    config = configparser.ConfigParser()
    fname = os.path.expanduser("~/.iSeriesAccess/cwb_userprefs.ini")
    if not os.path.exists(fname):
        os.makedirs(os.path.dirname(fname), 0o700, exist_ok=True)
    else:
        config.read(fname)
    section = (r"CWB_CURRUSER\Software\IBM\Client Access Express\CurrentVersion"
               r"\Environments\My Connections\{}\Communication".format(host))
    if not section in config:
        config.add_section(section)
    key = "Port lookup mode"
    port_mode = "attr_dwd:0x00000001" if port else "attr_dwd:0x00000002"
    if config[section].get(key) != port_mode:
        config[section][key] = port_mode
        with open(fname, "w") as f:
            config.write(f)


def _write_port_to_services(port):
    with open("/etc/services", "r+") as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            if not re.match(r"^as-database\s+", line):
                f.write(line)
        f.truncate()
        f.write("as-database {}/tcp # tap-db2\n".format(port))


def setup_port_configuration(config):
    host = config["db2_system"]
    port = config.get("db2_port")
    _write_userprefs(host, port)
    if port:
        _write_port_to_services(port)


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
