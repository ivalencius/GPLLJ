import numpy as np
import xarray as xr
from rich import print

import intake
import dask
from xmip.postprocessing import merge_variables
import regionmask 
import os

def get_query():
    """
    All variables are: ['hursmin' 'clt' 'hfls' 'hfss' 'hurs' 'hur' 'hursmax'
    'sfcWindmax' 'snc' 'vas' 'zg' 'wap' 'uas' 'ua' 'tslsi' 'tasmin' 'tasmax'
    'tas' 'ta' 'snw' 'sfcWind' 'rsus' 'va' 'rsds' 'rlut' 'hus' 'rlus' 'rlds' 
    'psl' 'prsn' 'huss' 'prc' 'pr' 'mrsos' 'mrso' 'mrro']
    
    Define variables via: https://clipc-services.ceda.ac.uk/dreq/mipVars.html

    There are two grids available, gr1 (100 km) and gr2 (250 km). Both have been regridded from gn.
    """
    # Historical query
    query = dict(
        experiment_id=['amip', 'amip-p4K'],
        table_id=['Eday'],
        variable_id=[
            'uas', # zonal wind (10 m)
            'vas', # meridional wind (10 m)
            'tslsi', # Surface temperature of all surfaces except ocean
            'tas', # Near surface (2 m) air temperature
            'tasmin', # Daily surface (2 m) min temperature
            'tasmax', # Daily surface (2 m) max temperature
            'psl', # Sea level pressure
            'pr', # Precipitation
            'prc', # Convective precipitation
            # Only available for gr2 (250 km Nominal Resolution)
            'ua', # zonal wind
            'va', # meridional wind
            'wap', # Omega (dp/dt)
            'zg', # Geopotential height
            'ta', # Air temperature
        ],
        grid_label=['gr2'], 
        member_id=['r1i1p1f1'],
        source_id=['GFDL-CM4'],
    )
    # Can also use from xmip.utils import google_cmip_col
    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    col = intake.open_esm_datastore(url)
    # cat_data = col.search(require_all_on=['source_id'], **query)
    cat_data = col.search(**query)
    print(cat_data.df['source_id'].unique())
    print(cat_data.df['member_id'].unique())
    print(cat_data.df['table_id'].unique())
    print(cat_data.df['variable_id'].unique())
    kwargs = {
        'zarr_kwargs':{
            'consolidated':True,
            'use_cftime':True
        },
        'aggregate':False,
    }
    ddict = cat_data.to_dataset_dict(**kwargs)
    ddict_merged = merge_variables(ddict)
    return ddict_merged
    

def bounds_mask():
    """
    Returns a mask for Central America -> Canada (includes Pacific and Atlantic)
    """
    conus_bnds = np.array([[-170, 10], [-170,  70], [-20,  70], [-20, 10]])
    names = ["Continental United States"]
    abbrevs = ['CONUS']
    bounds = regionmask.Regions([conus_bnds], names=names, abbrevs=abbrevs, name="US")
    return bounds

def _process(ds):
    """
    Squeeze the dataset and get rid of unnecessary variables
    """
    processed = (
        ds
        .squeeze()
        .sel(time=slice(None, '2015-01-01')) # remove the one day in 2015
        .drop_vars(['member_id', 'dcpp_init_year'])
    )
    return processed
        
def download(bounds, ddict_merged, save_file = '/scratch/valencig/GPLLJ-Scratch/GFDL-CM4.nc'):
    """
    Save the GFDL-CM4 data to `save_file`.
    We expand the dataset by dim `forcing` = ['+0K', '+4K']
    Note: sometimes this throws a "cannot combine ... use compat='override'"
    For some bizarre reason just rerunning the code fixes this...
    """
    keys = list(ddict_merged.keys())
    p4K_key = [k for k in keys if '-p4K' in k][0]
    ref_key = [k for k in keys if k != p4K_key][0]
    print('+0K key: ', ref_key)
    print('+4K key: ', p4K_key)
    ds_p0K = _process(ddict_merged[ref_key])
    ds_p4K = _process(ddict_merged[p4K_key])
    ds = xr.concat([ds_p0K, ds_p4K], dim='forcing').assign_coords(forcing=['+0K', '+4K'])
    mask = bounds.mask(ds.lon, ds.lat)
    ds = ds.where(mask == False, drop=True)
    print(ds)
    print(f'Saving to: {save_file}')
    ds.to_netcdf(save_file)
    size_gb = os.path.getsize(save_file) / 1024**3 # bytes to GB
    print(f'Size of file (GB): {size_gb}')
        
if __name__ == '__main__':
    ddict_merged = get_query()
    bounds = bounds_mask()
    download(bounds, ddict_merged)