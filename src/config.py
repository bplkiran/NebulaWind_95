from dataclasses import dataclass
import math

GROUP_TO_TURBINES = {
    "data_group_1.csv": [1, 2, 3, 4, 5],
    "data_group_2.csv": [6, 7, 8, 9, 10],
    "data_group_3.csv": [11, 12, 13, 14, 15],
}

MAX_WIND_SPEED_MS = 40.0
MAX_POWER_MW = 5.0
IQR_MULTIPLIER = 1.5
ANOMALY_Z_THRESHOLD = 2.0
HOURS_PER_MEASUREMENT = 1
PI = math.pi
