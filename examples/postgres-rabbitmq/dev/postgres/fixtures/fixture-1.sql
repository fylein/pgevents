create table users (
    id varchar(255) primary key,
    full_name varchar(255) not null,
    dob timestamp with time zone not null,
    email varchar(255) not null,
    is_email_verified boolean default false not null
);

alter table users replica identity full;
select pg_create_logical_replication_slot('pgevents', 'wal2json');
