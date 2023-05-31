import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
@transformer
def transform(uber_data, *args, **kwargs):
    uber_data['tpep_pickup_datetime'] = pd.to_datetime(uber_data['tpep_pickup_datetime'])
    uber_data['tpep_dropoff_datetime'] = pd.to_datetime(uber_data['tpep_dropoff_datetime'])
    uber_data = uber_data.drop_duplicates().reset_index(drop=True)
    uber_data['trip_id'] = uber_data.index
    datetime_dim = uber_data[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['tpep_pickup_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['tpep_pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['tpep_pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['tpep_pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['tpep_pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['tpep_drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['tpep_drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['tpep_drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['tpep_drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['tpep_drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'tpep_pickup_hour', 'tpep_pickup_day', 'tpep_pickup_month', 'tpep_pickup_year', 'tpep_pickup_weekday',  
                             'tpep_dropoff_datetime', 'tpep_drop_hour', 'tpep_drop_day', 'tpep_drop_month', 'tpep_drop_year', 'tpep_drop_weekday']]
    passenger_count_dim = uber_data[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]
    trip_distance_dim = uber_data[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']] 
    rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }    
    rate_code_dim = uber_data[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_Name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_Name']] 
    pickup_location_dim = uber_data[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']]
    dropoff_location_dim = uber_data[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']] 
    payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
    }
    payment_type_dim = uber_data[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]
    fact_table = uber_data.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
             .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
             .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
             .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
             .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
             .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]
    return {"datetime_dim": datetime_dim.to_dict(orient="dict"),
    "passenger_count_dim": passenger_count_dim.to_dict(orient="dict"),
    "trip_distance_dim": trip_distance_dim.to_dict(orient="dict"),
    "rate_code_dim": rate_code_dim.to_dict(orient="dict"),
    "pickup_location_dim": pickup_location_dim.to_dict(orient="dict"),
    "dropoff_location_dim": dropoff_location_dim.to_dict(orient="dict"),
    "payment_type_dim": payment_type_dim.to_dict(orient="dict"),
    "fact_table": fact_table.to_dict(orient="dict")
    }

@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'
