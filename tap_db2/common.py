import pyodbc
import backoff

# pylint: disable=no-member


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
