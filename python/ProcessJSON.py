import os
import fnmatch
import numpy as np
import pandas as pd

FIELDS = [
    'DATE-OBS',
    'DTCALDAT',
    'DTTELESC',
    'DTINSTRU',
    'OBSTYPE',
    'PROCTYPE',
    'PRODTYPE',
    'DTSITE',
    'OBSERVAT',
    #'REFERENCE',
    #'FILESIZE',
    #'MD5SUM',
    'DTACQNAM',
    'DTPROPID',
    #'PI',
    #'RELEASE_DATE',
    'RA',
    'DEC',
    #'FOOTPRINT',
    'FILTER',
    'EXPTIME', #'EXPOSURE', 
    'OBSMODE',
    'SEEING',
    #'DEPTH',
    #'SURVEYID',
    'OBJECT',
    ]



# HDF5 storage of dataframe metadata from 
# https://stackoverflow.com/questions/29129095/save-additional-attributes-in-pandas-dataframe/29130146#29130146
# note: needed to 'pip install --upgrade tables' for HDFStore
def h5store(filename, df, **kwargs):
    store = pd.HDFStore(filename)
    store.put('mydata', df)
    store.get_storer('mydata').attrs.metadata = kwargs
    store.close()

def h5load(store):
    data = store['mydata']
    metadata = store.get_storer('mydata').attrs.metadata
    return data, metadata

class ProcessJSON(object):

    def __init__(self, datadir,
                 savdir='~/pandas-snapshots',
                 file_hdr='local_file'):
        self._datadir   = datadir
        self._savdir    = os.path.expanduser(savdir)
        self._file_hdr  = file_hdr
        self._txtfmt    = 'DATE:{}, {} FILES'  # date, number of files that date
        self._savfmt    = '{}/{}-processed.hdf5' # savdir, date
        self._errmsg    = 'ERROR: Need to run .run() method first!'
        self._error_group_col = ('ERROR: grouping column {} in file {}'
                                 ' does not have a unique value')
        self._metadata        = {}
        self._date            = None
        self._important       = FIELDS
        self._processed       = None
        self._group_col       = None
        self._num             = None
        self._num_to_read     = None
        self._full_dataframe  = None
        self._multi_group_cols = None
        self._force_overwrite  = False
        
        os.makedirs(self._savdir, exist_ok=True)
        
    def _get_file_list(self):
        file_list = []
        for dirpath, dirs, files in os.walk(os.path.join(self._datadir,
                                                         self._date)): 
            for filename in fnmatch.filter(files, '*.json'):
                file_list.append(os.path.join(dirpath, filename))
        self._num = min(len(file_list), self._num_to_read)
        self._file_list = file_list
               
    def _process(self):
        '''process group of json files , save dataframe to disk'''
        
        self._get_file_list()
        print('processing ', self._txtfmt.format(self._date, self._num))
        
        # if important keys are provided, make a dummy starting dataframe
        # with those keys
        dd = [] if self._important == None else [pd.DataFrame(columns=self._important)]

        
        #!for k in range(self._num):
        for filename in self._file_list):
            jj = pd.read_json(filename)
            
            # verify the grouping-column value is unique and not missing
            # in this file across the HDUs, otherwise assert an error; 
            # TODO: make this a try/except: save bad filenames and keep moving
            assert jj[self._group_col].nunique() == 1, \
                self._error_group_col.format(self._group_col,filename)
            
            # if existing and unique, broadcast the grouping-column value
            # to the entire grouping column: this is required for proper
            # grouping later;
            # usually grouping column is 'DTINSTRU', the instrument name
            jj[self._group_col] = jj[self._group_col].dropna().iloc[0]
            
            # add the file-name column to the dataframe: 
            # this is required for grouping HDUs by filename
            jj[self._file_hdr] = filename[47:]
            
            dd.append(jj)
            
        # if important keys are provided, cull the dataframe with those keys: 
        # do this AFTER concat with dummy frame with all the important keys, 
        # otherwise smaller frames may not have all of the keys
        self._processed = pd.concat(dd) if self._important == None else \
                          pd.concat(dd)[self._important]
            
        self._metadata['num_files']   = self._num
        self._metadata['date_record'] = self._date
        h5store(self._savfile, self._processed, **self._metadata)
        
    def _get_data(self, date):
        self._date = date
        self._savfile = self._savfmt.format(self._savdir, self._date)
        if (os.path.isfile(self._savfile) and not self._force_overwrite):
            print('reading {} from disk'.format(self._savfile))
            with pd.HDFStore(self._savfile) as store:
                self._processed, self._metadata = h5load(store)
        else:
            self._process()
                    
    def run(self, date_range, group_col='DTINSTRU', important=None, 
            num_to_read=None, force_overwrite=False):  
        '''
        date_range:      list of date directories to read
        group_col:       column name on which to group: usually 'DTINSTRU'
        important:       list of columns to keep in processed dataframes
        num_to_read:     number of files to read 
                         (default: read all files in each date directory)
        force_overwrite: if processed dataframe exists on disk, 
                         overwrite if True (default: False)
        '''
        raw = []
        self._num_to_read = np.iinfo(np.int32).max if num_to_read == None else \
                            num_to_read
        self._group_col   = group_col
        self._multi_group_cols  = [self._group_col, self._file_hdr]

        # add file-header column for filename: 
        # important not to use append() method here: it breaks things
        self._important = important + [self._file_hdr] 
        self._force_overwrite = force_overwrite
        
        # ensure date_range is a list if a scalar is input
        date_range = date_range if isinstance(date_range, list) else [date_range]
        
        for k in date_range:
            self._get_data(k)
            raw.append(self._processed)
        self._full_dataframe = pd.concat(raw)
        
    @property
    def get_full_dataframe(self):
        assert self._full_dataframe is not None, self._errmsg
        return self._full_dataframe
    
    @property
    def get_instr_vs_fields_unique_all_data(self):
        # TODO: this needs to be generalized so user can define the top rows 
        #       for display: 
        #       this will be broken when the 'important' list changes
        gg = self.get_full_dataframe.groupby(self._group_col).nunique().T
        indx = list(gg.index)
        # reorder rows to get similar rows at top for direct comparison
        indx = [self._file_hdr,'DTACQNAM'] + indx[:12] + indx[13:-1]
        return gg.loc[indx,:]
    
    @property
    def get_HDU_uniqueness_per_file(self):
        gg = self.get_full_dataframe.groupby(self._multi_group_cols).nunique()
        # drop corrupted (by nunique()) grouping columns
        gg = gg.drop(self._multi_group_cols, axis=1)
        # reset index, drop unnecessary local_file column
        gg.reset_index().drop(self._multi_group_cols[1], axis=1)  
        return gg.groupby(self._multi_group_cols[0]).\
                  agg(['min','max','mean','std']).round(2).stack().T
    
    @property
    def get_all_fields(self):
        return self._important
    
    @property    
    def get_HDU_stats(self):
        gg = self.get_full_dataframe.groupby(self._multi_group_cols).size()
        return gg.groupby(self._group_col).agg(['min','max','mean','std']).\
                                         rename_axis('HDU stats:', axis=1)
    
    def get_num_files_writing_fields(self, instr=True, percent=True):
        '''
        instr:   if True, list percentages (or raw numbers) of files per 
                 instrument that write each field, if False list total
                 number of files (or percentages) over ALL instruments 
                 (default=True)
        percent: if True, list percentages of files that write each field, 
                 if False, list raw numbers of files (default=True)
        '''
        zz = self.get_full_dataframe.groupby(self._multi_group_cols).nunique() > 0
        if not instr:
            gg = zz.sum()
            return (gg/gg[self._file_hdr]*100).round(2) if percent else gg
        else:
            gg = zz.drop(['DTINSTRU'], axis=1).\
                 rename(columns={self._file_hdr:'COUNT'}).\
                 reset_index().drop(self._file_hdr, axis=1)
            gg = gg.groupby('DTINSTRU').sum().T
            return (gg/gg.loc['COUNT']*100).round(2) if percent else gg

    def get_unique_values_of_field(self, field):
        return list(self.get_full_dataframe[field].dropna().unique())
    
    def get_num_unique_values_by_keys(self, field1, field2):
        gg = self.get_full_dataframe.groupby([field1, field2]).nunique()
        return pd.DataFrame(gg.loc[:, self._file_hdr])\
                 .rename(columns={self._file_hdr:'TOTAL OCCURRENCES'})
