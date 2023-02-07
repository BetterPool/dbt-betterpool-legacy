## dbt models for `dbt-betterpool`

### How to set up a local enviorment:
To get up and running with this project:
1. Install dbt using [these instructions](https://docs.getdbt.com/docs/installation). When installing dbt with brew or pip, keep in mind that our adapter is Snowflake.

2. Clone this repository. If you need extra help, see [these instructions](https://docs.getdbt.com/docs/use-an-existing-project).

3. Before changing into the directory of the repository, run the following: 
```bash
$ dbt init
```
This command will create a .dbt folder which will include a profiles.yml file which will be used to establish a connection to the data warehouse (see step 5).

4. Change into the `dbt-betterpool-v2` directory from the command line:
```bash
$ cd dbt-betterpool-v2
```

5. Set up a profile called `dbt-betterpool-v2` to connect to a data warehouse by
  following [these instructions](https://docs.getdbt.com/docs/configure-your-profile).
  The .dbt/profiles.yml file is used to create a connection to a Data Warehouse of your choice. In our case we are currently using a Snowflake Data Warehouse. Documentation on how to properly format the profiles.yml file for a Snowflake connection can be found [here](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup).

6. Ensure your profile is setup correctly from the command line:
```bash
$ dbt debug
```

### Useful DBT Commands:

Run the models:
```bash
$ dbt run
```

Test the output of the models:
```bash
$ dbt test
```

Generate documentation for the project:
```bash
$ dbt docs generate
```

View the documentation for the project:
```bash
$ dbt docs serve
```

---
For more information on dbt:
- Read the [introduction to dbt](https://dbt.readme.io/docs/introduction).
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint).
---
