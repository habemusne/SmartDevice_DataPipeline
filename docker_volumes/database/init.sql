create table historical (
  id serial primary key,
  user_id integer,
  zipcode integer,
  latitude real,
  longitude real,
  city text,
  state text,
  area text
);

create table anomaly (
  id serial primary key,
  user_id integer,
  heart_rate integer,
  recorded timestamp with time zone
);
