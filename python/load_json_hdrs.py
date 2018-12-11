#! /usr/bin/env python
'''\
Load JSON format of FITS headers into pandas (HDF5 disk storage). Do analysis
'''

import os, sys, string, argparse, logging
import csv
from ProcessJSON import ProcessJSON, FIELDS

def loadJson(json_files, topdir=None, resultsdir='~/pandasresults'):
    dates = [str(x) for x in range(20170701, 20170726)]
    num = None #100  # 'None' to get all files
    force_overwrite = True
    proc = ProcessJSON(topdir)
    proc.run(dates,
             important=FIELDS, group_col='DTINSTRU', num_to_read=num,
             force_overwrite=force_overwrite)
    outdir = os.path.expanduser(resultsdir)

    ff=proc.get_num_files_writing_fields(instr=False, percent=False)
    dirname = os.path.join(outdir,'csv')
    os.makedirs(dirname, exist_ok=True)
    ff.to_csv(os.path.join(dirname,'get_num_files_writing_fields.csv'))

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
        epilog='EXAMPLE: %(prog)s a b"'
        )
    parser.add_argument('-t', '--topdir',
                        help='Dir containing json files (any level below)',
                        )
    parser.add_argument('-j', '--jlist',
                        type=argparse.FileType('rt'),
                        help=('File containing list of JSON files to load.'
                        ))
    parser.add_argument('-q', '--quality', help='Processing quality',
                        choices=['low','medium','high'], default='high')
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


    loadJson(None, topdir=args.topdir)

if __name__ == '__main__':
    main()
