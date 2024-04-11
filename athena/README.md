This repositories was created to store code and usage instructions based on requirements:
    Currently, the volume of the histories table in the database is increasing day by day, leading to an increase in the monthly bill. These histories tables are seldom accessed. Therefore, migrating that data to S3 and using Athena to query it when needed not only helps optimize storage but also leads to up to 90% savings on the monthly bill.

Note: Athena only queries on csv files and does not save data anywhere!


