import json
import requests
from enum import Enum

URL = 'https://archive-api.open-meteo.com'


class Scale(Enum):
    HOURLY = 'hourly'
    DAILY = 'daily'


WEATHER_METRIC_LIST = [
    'temperature_2m',
    'relative_humidity_2m',
    'dew_point_2m',
    'weather_code',
    'wind_speed_10m',
    'wind_direction_10m',
    'wind_gusts_10m',
    'surface_pressure',
    'precipitation',
]


def get_point_weather(point_coordinates: list, date_bounds: list, scale: Scale):
    endpoint = f'{URL}/v1/archive'
    params = {
        'latitude': point_coordinates[0],
        'longitude': point_coordinates[1],
        'start_date': date_bounds[0],
        'end_date': date_bounds[1],
        scale.value: ','.join(WEATHER_METRIC_LIST),
    }
    response = requests.get(endpoint, params=params, timeout=30)
    response.raise_for_status()
    return json.loads(response.text)


def get_sun_metric(point_coordinates: list, date_bounds: list):
    """
    Один запрос к Open-Meteo, чтобы получить:
      - hourly uv_index (для avg_uv_index)
      - daily daylight_duration, sunrise, sunset (для day_length_min)
    """
    endpoint = f'{URL}/v1/archive'
    params = {
        'latitude': point_coordinates[0],
        'longitude': point_coordinates[1],
        'start_date': date_bounds[0],
        'end_date': date_bounds[1],
        'hourly': 'uv_index',
        'daily': 'daylight_duration,sunrise,sunset',
        'timezone': 'UTC',
    }
    response = requests.get(endpoint, params=params, timeout=30)
    response.raise_for_status()
    return json.loads(response.text)


def flatten_response(api_response: dict, scale: str = 'hourly') -> list:
    common = {k: v for k, v in api_response.items() if k != scale}
    payload = api_response[scale]
    times = payload['time']
    metrics = {k: v for k, v in payload.items() if k != 'time'}
    result = []
    for i, t in enumerate(times):
        entry = {**common, 'time': t}
        for m, values in metrics.items():
            entry[m] = values[i]
        result.append(entry)
    return result