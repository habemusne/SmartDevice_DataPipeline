create table historical (
  user_id serial primary key,
  min_heart_rate integer,
  max_heart_rate integer
);

create table anomaly (
  user_id integer primary key,
  heart_rate integer,
  recorded timestamp with time zone
);
