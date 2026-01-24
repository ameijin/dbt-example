create schema if not exists raw;

create or replace table raw.users as
       select *, current_timestamp from read_csv_auto('data/users.csv');

create or replace table raw.subscriptions as
       select *, current_timestamp from read_csv_auto('data/subscriptions.csv');


select count(*) from raw.users;

