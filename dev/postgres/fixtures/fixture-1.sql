create table users (
    id int primary key,
    full_name text not null,
    company jsonb default '{"name": "Fyle"}'::jsonb,
    created_at timestamp with time zone default now(),
    updated_at timestamp with time zone default now()
);

alter table users replica identity full;

create publication events for all tables;