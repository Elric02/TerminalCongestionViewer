import polars as pl
import traj_dist.distance as tdist
import numpy as np
from sklearn.cluster import DBSCAN

terminal = 'linköping'
date = '2024-09-30'
providers = ['otraf']
filename = f'output/vehiclepositions_terminal_{terminal}_{"_".join(providers)}_{date}.csv'
print('reading', filename)
df = pl.read_csv(filename, schema_overrides={'trip_id': pl.Utf8, 'vehicle.id': pl.Utf8, 'route_id': pl.Utf8})
trip_ids = df.select(pl.col('trip_id')).unique().sort('trip_id').to_series().to_list()
trip_ids = [x for x in trip_ids if x is not None]
trip_coords = []
for trip in trip_ids:
    temp_df = df.filter(pl.col('trip_id') == trip)
    if temp_df.shape[0] < 5:
        continue
    trip_coords.append(temp_df.select(pl.col('longitude'), pl.col('latitude')).to_numpy())
print('raw_trips', len(trip_ids), 'traj_count', len(trip_coords))
if len(trip_coords) < 2:
    raise SystemExit('not enough trips')
sspd = tdist.pdist(trip_coords, metric='sspd', verbose=True)
print('sspd stats:', np.min(sspd), np.percentile(sspd, 1), np.percentile(sspd, 2), np.percentile(sspd, 5), np.percentile(sspd, 10), np.percentile(sspd, 20), np.percentile(sspd, 25), np.median(sspd), np.percentile(sspd, 75), np.percentile(sspd, 90), np.max(sspd))

n = len(trip_coords)
dist_matrix = np.zeros((n, n), dtype=float)
iu = np.triu_indices(n, k=1)
dist_matrix[iu] = sspd
dist_matrix[(iu[1], iu[0])] = sspd
for p in [1,2,5,10,15,20,25,30,40,50,60,70,80,90]:
    eps = max(np.percentile(sspd, p), 1e-7)
    labels = DBSCAN(eps=eps, min_samples=2, metric='precomputed').fit_predict(dist_matrix)
    uniq, counts = np.unique(labels, return_counts=True)
    print('p', p, 'eps', eps, 'clusters', len(uniq[uniq!=-1]) if len(uniq)>0 else 0, 'noise', counts[uniq==-1][0] if -1 in uniq else 0, 'labelcounts', list(zip(uniq, counts)))
