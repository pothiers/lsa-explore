#! /usr/bin/env python
'''\
Load JSON format of FITS headers into pandas (HDF5 disk storage). Do analysis

EXAMPLE:
./load_json_hdrs.py -j ~/data/json-scrape/pipe.files -t ~/data/json-scrape

ipython --simple-prompt --no-color-info

'''

import os, sys, string, argparse, logging
import csv
from ProcessJSON import ProcessJSON, FIELDS


def loadJson(json_files, topdir=None, resultsdir='~/pandasresults'):
    dates = [str(x) for x in range(20170701, 20170726)]
    num = None #100  # 'None' to get all files

    force_overwrite = True
    proc = ProcessJSON()
    proc.run(json_files,
             topdir=topdir,
             important=FIELDS, group_col='DTINSTRU', num_to_read=num,
             force_overwrite=force_overwrite)
    outdir = os.path.expanduser(resultsdir)

    dirname = os.path.join(outdir,'csv')
    os.makedirs(dirname, exist_ok=True)

    proc.get_num_files_writing_fields(instr=False, percent=False)\
        .to_csv(os.path.join(dirname,'get_num_files_writing_fields.csv'))
    proc.get_HDU_stats.to_csv(os.path.join(dirname,'get_HDU_stats.csv'))
    
    print('Wrote output files to: {}'.format(dirname))
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
    parser.add_argument('-j', '--jlist',
                        type=argparse.FileType('rt'),
                        help=('File containing list of JSON files to load.'
                              ' (relative paths to TOPDIR'
                        ))
    dft_topdir='~/data/json-fits-headers'
    parser.add_argument('-t', '--topdir',
                        help=('Root dir of filenames in JLIST.'
                              ' [default={}]').format(dft_topdir))

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
