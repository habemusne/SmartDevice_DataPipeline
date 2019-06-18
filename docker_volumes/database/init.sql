create table historical (
  id serial primary key,
  user_id integer,
  zipcode integer,
  latitude decimal,
  longitude decimal,
  city text,
  state text,
  area text
);
