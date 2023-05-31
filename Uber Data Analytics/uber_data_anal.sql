select vendorid, sum(fare_amount) ,avg(fare_amount) from fact_table group by vendorid;

select b.payment_type_name, avg(a.tip_amount) from fact_table a join payment_type_dim b on a.payment_type_id = b.payment_type_id group by b.payment_type_name;

describe fact_table;
select b.pickup_latitude,b.pickup_longitude, count(a.trip_id) from fact_table a join pickup_location_dim b on a.pickup_location_id = b.pickup_location_id group by b.pickup_longitude,b.pickup_latitude;
select b.passenger_count, count(a.trip_id) from fact_table a join passenger_count_dim b on a.passenger_count_id = b.passenger_count_id group by passenger_count;
select b.tpep_drop_hour, avg(a.fare_amount) from fact_table a join datetime_dim b on a.datetime_id = b.datetime_id group by tpep_drop_hour;

create table uber_data as
select f.vendorid from fact_table f 
join datetime_dim d on f.datetime_id = d.datetime_id
join passenger_count_dim p on f.passenger_count_id = p.passenger_count_id
join trip_distance_dim t on f.trip_distance_id = t.trip_distance_id
join rate_code_dim r on f.rate_code_id = r.rate_code_id
join pickup_location_dim pick on f.pickup_location_id=pick.pickup_location_id
join dropoff_location_dim dropoff on f.dropoff_location_id = dropoff.dropoff_location_id
join payment_type_dim pay on f.payment_type_id = pay.payment_type_id;
;




