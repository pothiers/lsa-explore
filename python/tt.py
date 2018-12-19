import ProcessJSON as pj
import pandas as pd
import numpy as np

jf1="~/data/json-fits-headers/pipe/20170701/ct4m/2017A-0260/c4d_170702_104324_ppi_z_v1.fits.json"
jf2='~/data/json-fits-headers/pipeline/Q20170221/K4N16B/20170109/k4n_170112_111105_ood_KXs_v1.fits.json'

df_list=[pd.DataFrame(columns=pj.FIELDS)]

df_list.append(pd.read_json(jf1))

df=pd.concat(df_list)[pj.FIELDS]

# infer_object: new in pd version 0.21.0
df.infer_objects().dtypes

# keep only HDU-0
df2 = df[:1]

df2.astype(dtype=pj.dtypes).dtypes
