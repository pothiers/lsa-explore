#! /usr/bin/env python3
'''\
Load JSON format of FITS headers into pandas (HDF5 disk storage). Do analysis

EXAMPLES

tail -n +1  pipe.files | head -n 10 > pipe.1-10.files
tail -n +11 pipe.files | head -n 10 > pipe.11-20.files

./load_json_hdrs.py ~/data/json-files/pipe.1-10.files  -t ~/data/json-files &
./load_json_hdrs.py ~/data/json-files/pipe.11-20.files -t ~/data/json-files &



ipython --simple-prompt --no-color-info

'''

import os, sys, string, argparse, logging
import csv
from pathlib import Path
from ProcessJSON import ProcessJSON, FIELDS


def loadJson(json_files, topdir=None, resultsdir='~/pandasresults'):
    dates = [str(x) for x in range(20170701, 20170726)]
    num = None #100  # 'None' to get all files

    jlistfile=Path(json_files.name)
    
    proc = ProcessJSON()
    proc.run(json_files,
             topdir=topdir,
             important=FIELDS, group_col='DTINSTRU', num_to_read=num,
             force_overwrite=True)
    outdir = os.path.expanduser(resultsdir)

    dirpath = Path(outdir,jlistfile.stem,'csv')
    os.makedirs(str(dirpath), exist_ok=True)

    proc.get_num_files_writing_fields(instr=False, percent=False).to_csv(
        str(dirpath / 'get_num_files_writing_fields.csv'))
    proc.get_HDU_stats.to_csv(str(dirpath / 'get_HDU_stats.csv'))

    proc.get_num_files_writing_fields(instr=True, percent=True).to_csv(
        str(dirpath
            / 'get_num_files_writing_fields(instr=True, percent=True).csv'))
    proc.get_instr_vs_fields_unique_all_data.to_csv(
        str(dirpath / 'get_instr_vs_fields_unique_all_data.csv'))
    proc.get_num_unique_values_by_keys('DTINSTRU', 'OBSTYPE').to_csv(
        str(dirpath / 'get_num_unique_values_by_keys_DTINSTRU_OBSTYPE.csv'))
    proc.get_num_unique_values_by_keys('DTINSTRU', 'FILTER').to_csv(
        str(dirpath / 'get_num_unique_values_by_keys_DTINSTRU_FILTER.csv'))
    
    print('Wrote output files to: {}'.format(dirpath))
    return proc


##############################################################################

def main_tt():
    cmd = 'MyProgram.py foo1 foo2'
    sys.argv = cmd.split()
    res = main()
    return res

def main():
    parser = argparse.ArgumentParser(
        #!version='1.0.1',
        description='My shiny new python program',
        epilog='EXAMPLE: ./load_json_hdrs.py -j ~/data/json-scrape/pipe.files -t ~/data/json-scrape'
        )
    parser.add_argument('jlist',
                        type=argparse.FileType('rt'),
                        help=('File containing list of JSON files to load.'
                              ' (relative paths to TOPDIR'
                        ))
    dft_topdir='~/data/json-fits-headers'
    parser.add_argument('-t', '--topdir',
                        default=dft_topdir,
                        help=('Root dir of filenames in JLIST.'
                              ' [default={}]').format(dft_topdir))
    dft_reportdir='~/reports_le'
    parser.add_argument('-r', '--reportdir',
                        default=dft_reportdir,
                        help=('Reports will be writtin here.'
                              ' [default={}]').format(dft_reportdir))


    dft_progress=int(1e3)
    parser.add_argument('--progress', type=int, default=dft_progress,
                        help=('Estimate completion before every N files.'
                              ' [default={}]').format(dft_progress))

    parser.add_argument('--loglevel',      help='Kind of diagnostic output',
                        choices = ['CRTICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='WARNING',
                        )
    args = parser.parse_args()

    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel) 
    logging.basicConfig(level = log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M'
                        )
    logging.debug('Debug output is enabled!!!')


    loadJson(args.jlist, topdir=args.topdir)

if __name__ == '__main__':
    main()
