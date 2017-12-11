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
        "db2_system": "127.0.0.1",
        "db2_uid": "your-db2-username",
        "db2_pwd": "your-db2-password"
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

---

Copyright &copy; 2017 Stitch
