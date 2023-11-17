create table users (
    id int primary key,
    full_name text not null
);

alter table users replica identity full;

create publication events for all tables;
