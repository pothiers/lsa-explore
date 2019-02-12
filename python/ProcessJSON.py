import os
import fnmatch
from datetime import datetime
import numpy as np
import pandas as pd
import pdb

from estimate_complete import Finish

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


def line_count(filename):
    '''Number of lines in FILENAME'''
    with open(filename) as f:
        cnt = 0
        for line in f: cnt += 1
    return cnt

def LINUX_line_count(filename):
    '''Number of lines in FILENAME'''
    return (int(subprocess.check_output(['wc', '-l', filename],
                                        universal_newlines=True).split()[0]))


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

    def __init__(self,
                 savdir='~/pandas-snapshots',
                 file_hdr='local_file', progress=1000, snapshot=2000): #! WS 12/14/18
        #self._datadir   = topdir
        self._savdir    = os.path.expanduser(savdir)
        self._file_hdr  = file_hdr
        self._txtfmt    = 'DATE:{}, {} FILES'  # date, number of files that date
        self._savfmt    = '{}/{}-processed.hdf5' # savdir, date
        self._errmsg    = 'ERROR: Need to run .run() method first!'
        self._error_group_col = ('ERROR: grouping column {} in file {}'
                                 ' does not have a unique value')
        self._metadata        = {}
#!        self._date            = None
        self._important       = FIELDS
        #self._processed       = None  #! WS 12/14/18 no longer needed
        self._group_col       = None
        self._num             = None
        self._num_to_read     = None
        self._full_dataframe  = None
        self._multi_group_cols = None
        self._force_overwrite  = False  #! WS 12/14/18 not currently implemented: will be trickier with new file looping
        self.progress = progress # Tell progress every N files.  #! WS 12/14/18
        self.snapshot = snapshot # Write snapshot N files.       #! WS 12/14/18
        
        os.makedirs(self._savdir, exist_ok=True)
        
    def _get_file_list(self):
        for dirpath, dirs, files in os.walk(os.path.join(self._datadir)): 
            for filename in fnmatch.filter(files, '*.json'):
                yield os.path.join(dirpath, filename)
               
    def _process(self, files, topdir='/'):
        #!pdb.set_trace()
        '''process group of json files , save dataframe to disk'''
        count = 0
        #! print('processing ', self._txtfmt.format(self._date, self._num))
        print('Collecting for fields: {}'.format(self._important))
        
        # if important keys are provided, make a dummy starting dataframe
        # with those keys
        dd = [] if self._important == None else [pd.DataFrame(columns=self._important)]
        dd_tot = []  # WS 12/14/18  this is the accumulator list; dd is the to-write list

        ec = Finish(line_count(files.name))
        print('[{}] DBG: Start reading files'.format(datetime.now().isoformat()))
        #!for k in range(self._num):
        #!for filename in self._get_file_list():
        for line in files:
            fname = line.strip()
            filename = os.path.join(topdir,fname)
            #!print('DBG: filename="{}"'.format(filename))

            
            if 0 == (count % self.progress):
                #!print('{} Processing file: {}'.format(count, filename))
                print('Estimate done: {}'.format(ec.est_complete(count)))
                
            count += 1
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
            jj[self._file_hdr] = os.path.basename(filename)
            
            dd.append(jj)

            #! WS 12/14/18 added test for end of list
            if 0 == (count % self.snapshot) or (count == line_count(files.name)):
                # Write snapshot (store as hdf5)
                #! WS 12/14/18 'count' to 'self.snapshot'
                self._metadata['num_files'] = self.snapshot  #@@@ WS 12/14/18 will have to modify for trailing files
                hdf_fname = '{}/snapshot-{}.hdf5'.format(self._savdir,count)
                df = pd.concat(dd)[self._important]  #! WS 12/14/18 important to strip fields here, for dd_tot
                h5store(hdf_fname, df, **self._metadata)     #! WS 12/14/18 
                dd_tot.append(df)                            #! WS 12/14/18  accumulate all df's
                dd = [pd.DataFrame(columns=self._important)] #! WS 12/14/18  reset dd for next write

        print('[{}] DBG: All files read'.format(datetime.now().isoformat()))
        # if important keys are provided, cull the dataframe with those keys: 
        # do this AFTER concat with dummy frame with all the important keys, 
        # otherwise smaller frames may not have all of the keys
        #! WS 12/14/18 this defines _full_dataframe, removed _process; changed dd to dd_tot
        #!pdb.set_trace()
        self._full_dataframe = pd.concat(dd_tot) if self._important == None else pd.concat(dd_tot)[self._important]

        # print('[{}] DBG-3'.format(datetime.now().isoformat()))       #! WS 12/14/18 no need for this
        #!self._metadata['num_files']   = self._num
        #!self._metadata['date_record'] = self._date
        # self._metadata['num_files'] = count                          #! WS 12/14/18 no need for this
        # hdf_fname = '{}/snapshot-{}.hdf5'.format(self._savdir,count) #! WS 12/14/18 no need for this
        # h5store(hdf_fname, self._processed, **self._metadata)  #! WS 12/14/18  don't want to write entire df!
        print('[{}] DBG-4'.format(datetime.now().isoformat()))
        print('Wrote snapshots to: {}'.format(self._savdir))
        
#!    def _get_data(self, date):
#!        #!self._date = date
#!        self._savfile = self._savfmt.format(self._savdir, self._date)
#!        if (os.path.isfile(self._savfile) and not self._force_overwrite):
#!            print('reading {} from disk'.format(self._savfile))
#!            with pd.HDFStore(self._savfile) as store:
#!                self._processed, self._metadata = h5load(store)
#!        else:
#!            self._process()
                    
    def run(self, files,
            topdir='/',
            group_col='DTINSTRU', important=None, 
            num_to_read=None, force_overwrite=False):  
        '''
        group_col:       column name on which to group: usually 'DTINSTRU'
        important:       list of columns to keep in processed dataframes
        num_to_read:     number of files to read 
                         (default: read all files in each date directory)
        force_overwrite: if processed dataframe exists on disk, 
                         overwrite if True (default: False)
        '''
        # raw = []   #! WS 12/14/18 OBE
        self._group_col   = group_col
        self._multi_group_cols  = [self._group_col, self._file_hdr]

        # add file-header column for filename: 
        # important not to use app1end() method here: it breaks things
        self._important = important + [self._file_hdr] 
        self._force_overwrite = force_overwrite
        
        # ensure date_range is a list if a scalar is input
        #!date_range = date_range if isinstance(date_range, list) else [date_range]
        #!
        #! for k in date_range:
        #!     self._get_data(k)
        #!     raw.append(self._processed)
 
        self._process(files, topdir=topdir) # @@@ Specify field types to improve performance

        # raw.append(self._processed)           #! WS 12/14/18 OBE
        # self._full_dataframe = pd.concat(raw) #! WS 12/14/18 OBE
        
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

    @property    
    def pct_files_with_values(self):
        """Like get_num_files_writing_fields(True, True), but simplier.
        """

        gpcol = self._group_col
        filecol = self._file_hdr
        
        gpcount = df2.groupby(gpcol).count()
        return gpcount.divide(gpcount[filecol], axis=0).T
