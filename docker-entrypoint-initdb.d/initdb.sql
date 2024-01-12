create database airflow_db;
create user airflow_user with password 'airflow_pass';
grant all privileges on database airflow_db to airflow_user;

create table public.app_store_topics (
    id serial primary key,
    app_name varchar(50) not null,
    app_id varchar(10) not null,
    country char(2) not null,
    lang char(2) not null,
    lda_data json not null,
    update_dt timestamp(0) not null,
    unique (app_name, app_id, country)
);

create table public.google_play_topics (
    id serial primary key,
    app_id varchar(50) not null,
    country char(2) not null,
    lang char(2) not null,
    lda_data json not null,
    update_dt timestamp(0) not null,
    unique (app_id, country)
);
