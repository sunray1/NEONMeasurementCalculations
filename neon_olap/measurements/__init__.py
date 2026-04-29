from .wind import *
from .temperature_air import *
from .barometric_pressure import *
from .temperature_infrared import *
from .precipitation_chem import *
from .particulate_matter import *

MEASUREMENTS = {
    # ---------- wind ----------    
    "wind_direction_mean": wind_direction_mean,
    "wind_direction_temporal_variance": wind_direction_temporal_variance,
    "wind_direction_elevational_variance": wind_direction_elevational_variance,

    "wind_speed_mean": wind_speed_mean,
    "wind_speed_maximum": wind_speed_maximum,
    "wind_speed_minimum": wind_speed_minimum,
    "wind_speed_temporal_variance": wind_speed_temporal_variance,
    "wind_speed_elevational_variance": wind_speed_elevational_variance,

    # ---------- temperature_air ----------
    "temperature_air_mean": temperature_air_mean,
    "temperature_air_maximum": temperature_air_maximum,
    "temperature_air_minimum": temperature_air_minimum,
    "temperature_air_temporal_variance": temperature_air_temporal_variance,
    "temperature_air_elevational_variance": temperature_air_elevational_variance,
    
    # ---------- barometric_pressure ----------
    "barometric_pressure_mean": barometric_pressure_mean,
    "barometric_pressure_maximum": barometric_pressure_maximum,
    "barometric_pressure_minimum": barometric_pressure_minimum,
    "barometric_pressure_temporal_variance": barometric_pressure_temporal_variance,
    
    # ---------- infrared temperature ----------
    "temperature_infrared_mean": temperature_infrared_mean,
    "temperature_infrared_maximum": temperature_infrared_maximum,
    "temperature_infrared_minimum": temperature_infrared_minimum,
    "temperature_infrared_temporal_variance": temperature_infrared_temporal_variance,
    "temperature_infrared_elevational_variance": temperature_infrared_elevational_variance,
    
    # ---------- precicpitation chemistry ----------
    "ammonium_concentration_precipitation": ammonium_concentration_precipitation,
    "bromide_concentration_precipitation": bromide_concentration_precipitation,
    "calcium_concentration_precipitation": calcium_concentration_precipitation,
    "chloride_concentration_precipitation": chloride_concentration_precipitation,
    "conductivity_precipitation": conductivity_precipitation,
    "magnesium_concentration_precipitation": magnesium_concentration_precipitation,
    "nitrate_concentration_precipitation": nitrate_concentration_precipitation,
    "ph_precipitation": ph_precipitation,
    "phosphate_concentration_precipitation": phosphate_concentration_precipitation,
    "potassium_concentration_precipitation": potassium_concentration_precipitation,
    "sodium_concentration_precipitation": sodium_concentration_precipitation,
    "sulfate_concentration_precipitation": sulfate_concentration_precipitation,
    
    # ---------- particulate matter ----------
    "particulate_matter_le_1_um_concentration": particulate_matter_le_1_um_concentration,
    "particulate_matter_le_10_um_concentration": particulate_matter_le_10_um_concentration,
    "particulate_matter_le_15_um_concentration": particulate_matter_le_15_um_concentration,
    "particulate_matter_le_25_um_concentration": particulate_matter_le_25_um_concentration,
    "particulate_matter_le_4_um_concentration": particulate_matter_le_4_um_concentration,
}