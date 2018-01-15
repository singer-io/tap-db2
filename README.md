# tap-db2

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from DB2
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

   ```
   pip install tap-db2
   ```

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
   {
       "host": "127.0.0.1",
       "user": "your-db2-username",
       "password": "your-db2-password"
   }
   ```

   If you need to use a custom port (the default being 8471), see [Custom
   Ports](#custom-ports) for more information.

   If you would like to limit discovery to a set of certain schemas, you can do
   so by providing the `"filter_schemas"` key in the configuration. This should
   be a CSV string of schemas. For example:

   ```json
   {
       "host": "127.0.0.1",
       ...
       "filter_schemas": "schema1,schema2,schema3"
   }
   ```

3. Run the tap in discovery mode

   ```
   tap-db2 -c config.json -d
   ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

4. Run the tap in sync mode

   ```
   tap-db2 -c config.json -p catalog.json
   ```

## Custom Ports

This tap supports using a custom port to connect to your DB2 instance, but
there are some important considerations. The IBM ODBC driver has a roundabout
way of determining the port to connect to. The process the tap takes to
configure the port is:

- Update the `~/.iSeriesAccess/cwb_userprefs.ini` file and set `Port lookup
  mode` to `1`, which tells the driver to use the `/etc/services` file as a
  lookup for the port to use.
- Update the `/etc/services` file to set the `as-database` port to whatever
  port has been chosen.

In order to write the port to `/etc/services`, the tap must be run as a user
that has write permissions on it.

The tap does not make any effort to restore these files to their original
settings. Be aware that the modifications to these files may affect any other
software on your system that may use them.

When you are ready to use your custom port, you can update your `config.json`
to use the `port` option, likeso:

```
{
  "host": "some-host",
  "port": 1234
}
```

---

Copyright &copy; 2017 Stitch
