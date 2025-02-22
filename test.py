import pykoda
import pandas as pd

# Contains 
static_data = pykoda.datautils.load_static_data('otraf', '2022_03_22', remove_unused_stations=True)

print(static_data)