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
