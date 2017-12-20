import os
import re
import shutil
import configparser
import pyodbc
import backoff

# pylint: disable=no-member

def _write_userprefs(host, port):
    """Creates or updates the ~/.iSeriesAccess/cwb_userprefs.ini file to
    specify a "Port lookup mode", which controls how the driver determines
    which port to use when connecting."""
    # This file and its values were found by chris@stitchdata.com using strace,
    # educated guesses, and hints from random docs on the internet. At the time
    # of this writing, I could not find any specific documentation on this file
    # or its options.
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
    """Modifies the /etc/services file to specify a port for connecting to
    DB2."""
    with open("/etc/services", "r+") as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            # as-database was found in
            # https://www.ibm.com/support/knowledgecenter/en/ssw_ibm_i_71/rzaii/rzaiiservicesandports.htm
            if not re.match(r"^as-database\s+", line):
                f.write(line)
        f.truncate()
        f.write("as-database {}/tcp # tap-db2\n".format(port))


def setup_port_configuration(config):
    host = config["host"]
    port = config.get("port")
    _write_userprefs(host, port)
    if port:
        _write_port_to_services(port)


@backoff.on_exception(backoff.expo,
                      (pyodbc.OperationalError),
                      max_tries=5,
                      factor=2)
def connection(config):
    # Docs on keywords this driver accepts:
    # https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/rzaik/rzaikconnstrkeywordsgeneralprop.htm
    return pyodbc.connect(
        driver="{iSeries Access ODBC Driver 64-bit}",
        system=config["host"],
        uid=config["user"],
        pwd=config["password"])


class get_cursor(object):
    def __init__(self, config):
        self.conn = connection(config)
        self.cur = self.conn.cursor()

    def __enter__(self):
        return self.cur

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.cur.close()
        self.conn.close()
