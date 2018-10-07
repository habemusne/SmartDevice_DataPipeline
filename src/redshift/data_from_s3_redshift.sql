-- create table for storing patients personal details
create table user_personal_details (
  name                       VARCHAR(255)     not null,
  device_id                  INTEGER          not null,
  home_zipcode               INTEGER          not null,
  contact_number             INTEGER          not null,
  emergency_contact_number   INTEGER          not null
);

-- create table for storing patients health details
create table user_health_details(
device_id                   INTEGER           not null,
min_heart_rate		          FLOAT             not null,
max_heart_rate              FLOAT             not null
);

-- create table for storing patients real time health details
create table daily_health_vitals(
device_id           INTEGER not null,
latitude            DOUBLE PRECISION not null,
longitude           DOUBLE PRESICION not null,
heart_rate          FLOAT not null,
time                TIMESTAMP not null
);

-- copy data from s3
copy daily_health_vitals from 's3://anshu-insight/rawData/part_<current_date>'
credentials 'aws_access_key_id=<>;aws_secret_access_key=<>'
csv;

