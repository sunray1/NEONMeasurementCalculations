from .wind import *

MEASUREMENTS = {
    "wind_direction_mean": wind_direction_mean,
    "wind_direction_temporal_variance": wind_direction_temporal_variance,
    "wind_direction_elevational_variance": wind_direction_elevational_variance,

    "wind_speed_mean": wind_speed_mean,
    "wind_speed_maximum": wind_speed_maximum,
    "wind_speed_minimum": wind_speed_minimum,
    "wind_speed_temporal_variance": wind_speed_temporal_variance,
    "wind_speed_elevational_variance": wind_speed_elevational_variance,
}