import ionex
import matplotlib.pyplot as plt

ds = ionex.read_ionex('C:/Users/ElricM/OneDrive - VTI/Thesis/TerminalCongestionViewer/TerminalCongestionViewer/tempdata/ionex/ESA0OPSRAP_20251620000_01D_01H_GIM.INX')
print(ds)
ionex.plot_tec_map(ds.tec.isel(time=0))
plt.show()

# Plot the time series for a specific latitude and longitude
ionex.plot_time_series(ds, lat=68.4418, lon=22.4435, variable='tec')
plt.show()