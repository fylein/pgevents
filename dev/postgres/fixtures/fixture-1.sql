create table users (
    id int primary key,
    full_name text not null
);

alter table users replica identity full;
select pg_create_logical_replication_slot('pgevents', 'wal2json');
