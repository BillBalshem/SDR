'''
VERSION 0.8

Created on October 2012

@author: pbradley

CHANGE LOG
-- 12/7/2012 0.2 (pbradley)  : Updated kvals list to reflect NDI fields vs CDC fields.  Updated MATCH_CRITERIA_x dictionaries.
-- 12/17/2012 0.3 (pbradley) : Updated OutputMatchData() method to remove hardcoded input field header dependency.  Changed PatientID to ID.
                           Added kvals as an argument.
-- 12/21/2012 0.4 (pbradley) : Changed output file to include M or P for Match or Possible match indicator.  Added possible match criteria.
                           Changed match value output to be just the value of the matched fields, not the entire record.
-- 01/03/2013 0.5 (pbradley) : Added support for New field within input file that will be used to prevent match comparisons
                               between "old" (i.e. existing records).
-- 01/08/2013 0.6 (pbradley) : removed trailing space in sqlfile.write() from ', ' to ','
-- 01/24/2013 0.7 (pbradley) : added scoreRec() method to system to score the completeness of a record and return the score in the output
                               file.  The last two fiels in the output file are the scores and the correspond to the IDs in the first two fields
-- 01/25/2013 0.8 (pbradley) : Updated MATCH_CRITERIA_1 to include LastName

Design Notes:
-- Match order dependencies; once a match is found the base record is no longer used in subsequent searches

TODO:
-- Need to consider the difference between NULL/missing fields vs differing data.  For example if there are two records where
   the FNAME, LNAME, and DOB match, where one has a SSN and the other's SSN field is NULL -- how should this be handled compared
   to a scenario where the other's SSN field is a DIFFERENT value.
'''
import sys
sys.path = ['c:/work/SDRmatch2/febrl-0.4.2'] + ['c:/work/SDRmatch2/libsvm'] + sys.path
import time
import os
import errno
import glob
import datetime
import argparse
import logging
import csv
from os.path import join as pjoin, isdir, isfile
from stringcmp import do_stringcmp
import random

'''
Dictionaries containing keys and minimum match values

REF kvals = ['LastName', 'FirstName', 'MiddleName','Suffix','DOB','Sex','Surname','SSN']
'''

MATCH_CRITERIA_1 = {'SSN': 1,
                    'LastName': 0.9}

MATCH_CRITERIA_2 = {'LastName': 1,
                    'FirstName': 1,
                     'DOB': 0.9,
                     'Surname': 1 }

MATCH_CRITERIA_3 = {'LastName': 0.85,
                     'FirstName': 0.85,
                     'DOB': 0.85,
                     'Surname': 0.85,
                     'MiddleName': 0.85 }

MATCH_CRITERIA_4 = { 'LastName': 0.85,
                     'DOB': 1,
                     'Sex': 1,
                     'SSN': 0.85 }

MATCH_CRITERIA_5 = { 'FirstName': 0.85,
                     'Surname': 0.85,
                     'DOB': 1,
                     'Sex': 1,
                     'SSN': 0.95 }

MATCH_CRITERIA_6 = { 'MiddleName': 0.85,
                     'Surname': 0.85,
                     'DOB': 1,
                     'Sex': 1,
                     'SSN': 0.95 }


CRITERIA_LIST = [MATCH_CRITERIA_1, MATCH_CRITERIA_2,
                 MATCH_CRITERIA_3, MATCH_CRITERIA_4,
                 MATCH_CRITERIA_5, MATCH_CRITERIA_6]

POSMATCH_CRITERIA_1 = {'LastName': 1,
                       'FirstName': 0.85,
                       'DOB': 1,
                       'Surname': 0.85 }

POSMATCH_CRITERIA_2 = {'LastName': 1,
                       'FirstName': 1,
                       'DOB': 1 }


POSBL_LIST = [POSMATCH_CRITERIA_1, POSMATCH_CRITERIA_2]

REC_VAL = {'SSN':10, 'LastName':10, 'DOB':5, 'FirstName':5, 'MiddleName':1,'Suffix':1,'Sex':1,'Surname':1}

class DataFile(object):
    def __init__(self, path):
        self.path = path
        self.fnames = []
    def lines(self):
        try:
            reader = csv.DictReader(open(self.path, 'rU'), dialect='excel', delimiter=',')
            logging.debug('in DataFile(): ' + str(reader.fieldnames))
            self.fnames = reader.fieldnames
            for line in reader:
                yield line
        except Exception, e:
            logging.error('****DataFile Exception***')
            logging.error(str(e))

    def close(self):
        try:
            with open(self.path, 'rU') as f:
                csv.reader(f, delimiter='\t')
        except Exception, e:
            logging.error('****DataFile Close Exception***')
            logging.error(str(e))

    def getfnames(self):
        return self.fnames
    @staticmethod
    def key(line):
        return tuple(line.strip().split()[2:6])
    @staticmethod
    def kval(line, kval):
        return line[kval]


def timeStamped(fname, fmt='%Y-%m-%d-%H-%M-%S_{fname}'):
    return datetime.datetime.now().strftime(fmt).format(fname=fname)


class DbFile(DataFile):
    def __init__(self, path):
        DataFile.__init__(self, path)
        self._keys = None
    def keys(self):
        if self._keys is None:
            self._keys = set(self.key(line) for line in open(self.path))
        return self._keys

class InFile(DataFile):
    def __init__(self, path):
        DataFile.__init__(self, path)
        self.mflag = False
        self.tmp_dict = dict()
        self.mresdict = dict()
        self.lastcriteria = {}
        self.resavg = 0
        self.iline_val = 0
        self.i2line_val = 0
    def filtered_lines(self, pdb):
        keys = pdb.keys()
        for line in self.lines():
            if self.key(line) in keys:
                yield line

    def getmres_ave(self):
        total = 0
        count = 0
        for kval  in self.mresdict:
            total += self.mresdict.get(kval)
            count += 1
        return total / count

    # TODO
    # ... create i2line_mod1 with fname and lname switched
    # ... create i2line_mod2 with mm and dd switched
    # ... swamp mmdd and yyyy of DOB
    def matchRecwChg1 (self, kvals, i2line, iline):
        try:
            i2line_mod = i2line
            fname = i2line['FirstName']
            lname = i2line['LastName']
            i2line_mod['FirstName'] = lname
            i2line_mod['LastName'] = fname
            self.mresdict.clear()
            for kval in kvals: #kvals.split(','):
                self.mresdict[kval] = round(do_stringcmp('jaro', i2line_mod[kval], iline[kval])[0], 2)
            #return mresdict
        except Exception, e:
            logging.error('*****matchRecwChg1 Exception*********')
            logging.error(str(e))

    def matchRec (self, kvals, i2line, iline):
        try:
            self.mresdict.clear()
            for kval in kvals: #kvals.split(','):
                self.mresdict[kval] = round(do_stringcmp('jaro', i2line[kval], iline[kval])[0], 2)
                #logging.debug('kval:' + str(kval) + 'mresdict: ' + str(self.mresdict[kval]))
            #return mresdict
        except Exception, e:
            logging.error('*****matchRec Exception*********')
            logging.error(str(e))

    def scoreRec (self, kvals, i2line, iline):
        ''' Creates a score for iline and i2line based on the completeness
        and weight of each field
        REF kvals = ['LastName', 'FirstName', 'MiddleName','Suffix','DOB','Sex','Surname','SSN']
        '''
        try:
            self.iline_val = 0
            self.i2line_val = 0
            for rline in [iline, i2line]:
                rline_val = 0
                for kval in kvals:
                    if kval is 'SSN':
                        #print 'SSN :' + str(rline[kval]) + '... ' + str(rline[kval][0:3])
                        if rline[kval].__len__() < 9:
                            continue
                        elif str(rline[kval][0:3]) == '000':
                            continue
                        elif str(rline[kval][0:3]) == '666':
                            continue
                        elif str(rline[kval][0:3]) >= '900':
                            continue
                        elif str(rline[kval][3:5]) == '00':
                            continue
                        elif str(rline[kval][5:9]) == '0000':
                            continue
                        else:
                            rline_val = rline_val + 8
                    elif kval is 'LastName':
                        if rline[kval].__len__() == 0:
                            continue
                        elif rline[kval].__len__() == 1:
                            rline_val = rline_val + 4
                        elif rline[kval].__len__() > 1:
                            rline_val = rline_val + 8
                    elif kval is 'FirstName':
                        if rline[kval].__len__() == 0:
                            continue
                        elif rline[kval].__len__() == 1:
                            rline_val = rline_val + 2
                        elif rline[kval].__len__() > 1:
                            rline_val = rline_val + 4
                    elif kval is 'DOB':
                        if rline[kval].__len__() == 0:
                            continue
                        elif rline[kval].__len__() == 1:
                            rline_val = rline_val + 2
                        elif rline[kval].__len__() > 1:
                            rline_val = rline_val + 4
                    elif kval is 'Sex':
                        if rline[kval].__len__() == 0:
                            continue
                        elif rline[kval].__len__() == 1:
                            rline_val = rline_val + 1
                        elif rline[kval].__len__() > 1:
                            rline_val = rline_val + 2
                    elif kval is 'Surname':
                        if rline[kval].__len__() == 0:
                            continue
                        elif rline[kval].__len__() == 1:
                            rline_val = rline_val + 1
                        elif rline[kval].__len__() > 1:
                            rline_val = rline_val + 2
                    elif kval is 'Suffix':
                        if rline[kval].__len__() == 0:
                            continue
                        elif rline[kval].__len__() == 1:
                            rline_val = rline_val + 1
                        elif rline[kval].__len__() > 1:
                            rline_val = rline_val + 2
                    elif kval is 'MiddleName':
                        if rline[kval].__len__() == 0:
                            continue
                        elif rline[kval].__len__() == 1:
                            rline_val = rline_val + 1
                        elif rline[kval].__len__() > 1:
                            rline_val = rline_val + 2
                self.iline_val = self.i2line_val
                self.i2line_val = rline_val
        except Exception, e:
            logging.error('*****scoreRec Exception*********')
            logging.error(str(e))      
        
        
    def checkPosbl(self):
        # Check for possible  matches. This checks to see if 4 of the fields have a 
        # match value greater than 0.8
        try:
            pcnt = 0
            i2match = False
            for criteria in POSBL_LIST:
                self.resavg = 0
                ccount = 0
                for kval in criteria:  # criteria.items()
                    if self.mresdict.get(kval) >= criteria.get(kval):
                        i2match = True
                        self.resavg = self.resavg + self.mresdict.get(kval)
                        ccount = ccount + 1
                    else:
                        i2match = False
                        break
                if i2match:
                    self.resavg = self.resavg / ccount
                    self.lastcriteria = criteria
                    return i2match
                else:
                    continue
            return i2match # False
        except Exception, e:
            logging.error('*****checkPosbl Exception*********')
            logging.error(str(e))

    def checkCriteria (self):
        try:
            i2match = False
            for criteria in CRITERIA_LIST:
                self.resavg = 0
                ccount = 0
                for kval in criteria:  # criteria.items()
                    if self.mresdict.get(kval) >= criteria.get(kval):
                        i2match = True
                        self.resavg = self.resavg + self.mresdict.get(kval)
                        ccount = ccount + 1
                    else:
                        i2match = False
                        break
                if i2match:
                    self.resavg = self.resavg / ccount
                    self.lastcriteria = criteria
                    return i2match
                else:
                    continue
            return i2match # False
        except Exception, e:
            logging.error('*****checkCriteria Exception*********')
            logging.error(str(e))

    def outputMatchData (self, i2match, iline_index, i2line_index, iline, i2line, outfile, sqlfile, kvals):
        #ptble = {False:'N', True: 'N', 'Possible':'Y'}
        try:
            possible = 'M'
            if i2match:
                if i2match is 'Possible': possible = 'P'                
                self.mflag = True
                if self.tmp_dict.has_key(iline_index):
                    self.tmp_dict[i2line_index] = self.tmp_dict.get(iline_index)
                elif self.tmp_dict.has_key(i2line_index):
                    self.tmp_dict[iline_index] = self.tmp_dict.get(i2line_index)
                else:
                    self.tmp_dict[iline_index] = iline_index
                    self.tmp_dict[i2line_index] = iline_index
                outfile.write(str(iline_index) + ', ' + str(i2line_index) + ', '
                              + str(iline['ID']) + ', '
                              + str(i2line['ID']) + ', '
                              + str(iline['LastName']) + ', '
                              + str(iline['FirstName']) + ', '
                              + str(self.resavg) + ', '
                              + str(self.lastcriteria) + '\n'
                              + '-------> ' + str(self.mresdict) + '\n')
                sqlfile.write(str(iline['ID']) + ','
                              + str(i2line['ID']) + ','
                              + str(self.resavg) + ','
                              + possible + ','
                              + str(self.iline_val) + ','
                              + str(self.i2line_val) + '\n')
            elif not self.tmp_dict.has_key(iline_index):
                self.tmp_dict[iline_index] = iline_index
            mystring = str(self.tmp_dict.get(iline_index)) + ', ' + possible + ', '
            for val in kvals:
                mystring = mystring + ', ' + str(iline[val])
            mystring = mystring + '\n'
            outfile.write(mystring)
        except Exception, e:
            logging.error('*****outputMatchData Exception*********')
            logging.error(str(e))

    def dedup1file (self, inf2, kvals, logfile, sfile):
        # Compare select fields between every record in ONE file
        # - note, the inner loop breaks after the first match so the base 
        #   record is only match to, at most, one other record, THUS
        #   if three records match, the 2nd matched record will be
        #   matched to the 3rd matched record -- the 1st will never
        #   be matched to the 3rd.  Consequently, it is feasible
        #   although not probable that some matches or probable matches
        #   may be missed.  For example, if 1st and 2nd are matches, and
        #   1st and 3rd are matches, but 2nd and 3rd are not matches or
        #   are probable matches, this will change the result.  
        try:
            iline_index = 1
            for iline in self.lines():
                #
                # TODO: check for empty file or file with only one record
                #
                i2line_index = 1
                self.mflag = False
                #logging.debug('HERE1: ' + str(iline))
                for i2line in inf2.lines():
                    i2match = False
                    # only compare x to x+1 or greater
                    if i2line_index < iline_index + 1:
                        i2line_index += 1
                        continue
                    # check to make sure that either iline or i2line record is "new"
                    if iline['New'] == i2line['New'] == 'N':
                        i2line_index += 1
                        continue
                    # regular match
                    self.matchRec(kvals, i2line, iline)
                    self.scoreRec(kvals, i2line, iline)
                    logfile.write('R: ' + str(iline_index) + ', ' + str(i2line_index) + ', ' + str(self.mresdict) + '\n')
                    if self.checkCriteria():
                        i2match = 'True'
                        self.outputMatchData(i2match, iline_index, i2line_index, iline, i2line, logfile, sfile, kvals)
                        break  # see comments above ... remove this BREAK for SQL
                    # probably/possible match
                    if self.checkPosbl():
                        i2match = 'Possible'
                        self.outputMatchData(i2match, iline_index, i2line_index, iline, i2line, logfile, sfile, kvals)
                        break  # see comments above ... remove this BREAK for SQL
                    # match on modified data 
                    self.matchRecwChg1(kvals, i2line, iline)
                    logfile.write('R: ' + str(iline_index) + ', ' + str(i2line_index) + ', ' + str(self.mresdict) + '\n')
                    if self.checkCriteria():
                        i2match = 'Possible'
                        self.outputMatchData(i2match, iline_index, i2line_index, iline, i2line, logfile, sfile, kvals)
                        break  # see comments above ... remove this BREAK for SQL 
                    i2line_index += 1
                if not self.mflag:
                    self.outputMatchData(i2match, iline_index, i2line_index, iline, i2line, logfile, sfile, kvals)
                iline_index += 1
            #
            for kval in self.tmp_dict:
                logfile.write('index: ' + str(kval) + ' matches: ' + str(self.tmp_dict.get(kval)) + '\n')
        except Exception, e:
            logging.error('***** dedup1file exception*********')
            logging.error(str(e))

    def dedup2files (self, inf2, kvals, outfile, sfile):
        #
        try:
            iline_index = 1

            for iline in self.lines():
                i2line_index = 1
                self.mflag = False
                logging.debug('in dedup2files(): ' + str(iline))
                for i2line in inf2.lines():
                    i2match = False
                    diff = [key for key, val1 in iline.iteritems() if val1 != i2line[key]]  # skip self/same line
                    if len(diff) is 0:
                        logging.debug('LINE IS THE SAME')
                    else:
                        self.matchRec(kvals, i2line, iline)
                        i2match = self.checkCriteria()
                        if i2match:
                            self.outputMatchData(i2match, iline_index, i2line_index, iline, outfile, sfile, kvals)
                    i2line_index += 1
                if not self.mflag:
                    self.outputMatchData(i2match, iline_index, i2line_index, iline, outfile, sfile, kvals)
                iline_index += 1
        except Exception, e:
            logging.error('***** dedup1file exception*********')
            logging.error(str(e))

    def dump(self, output, pdb):
        for line in self.filtered_lines(pdb):
            logging.debug("***LINE***: " + line)
            output.write(line)
        output.close()

class Output(DataFile):
    def __init__(self, path):
        DataFile.__init__(self, path)
        self.ofile = open(path, "w")
    def write(self, *args):
        return self.ofile.write(*args)
    def close(self):
        self.ofile.close()

class Directory(object):
    def __init__(self, path):
        self.path = path
    def names(self, extension=None):
        for name in os.listdir(self.path):
            if extension is None or name.endswith(extension):
                yield name

def process(in_file, kvals, out_dir, log_dir):
    inf = InFile(in_file)
    inf2 = InFile(in_file)
    sqlfile = Output(out_dir + '/out_' + os.path.basename(in_file)) #result used by SQL SSIS
    logf = Output(log_dir + '/log_' + timeStamped(os.path.basename(in_file))) #for debuggin
    inf.dedup1file(inf2, kvals, logfile=logf, sfile=sqlfile)
    logf.close()
    sqlfile.close()
    inf.close()
    inf2.close()

LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}

def main():
    use = "usage: %prog [options] arg1 arg2 arg3 arg4"
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--logging-level', help='Logging level')
    parser.add_argument('-f', '--logging-file', help='Logging file name')
    parser.add_argument('wrkdir', help='working direcotry')
    parser.add_argument('filecnt', help='number of files to process')
    #parser.add_argument('kvals', help='list of keys to use')
    #kvals = ['LastName', 'FirstName', 'DOB', 'Sex', 'MomMaiden', 'MomLast', 'MomFirst']
    kvals = ['LastName', 'FirstName', 'MiddleName','Suffix','DOB','Sex','Surname','SSN']
    args = parser.parse_args()
    logging_level = LOGGING_LEVELS.get(args.logging_level, logging.NOTSET)
    logging.basicConfig(level=logging_level,
                      filename=args.logging_file,
                      format='%(asctime)s %(levelname)s: %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S')
    try:
        os.chdir(args.wrkdir)
        outdir = os.path.join(args.wrkdir, 'output')
        os.makedirs(outdir)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    try:
        logdir = os.path.join(args.wrkdir, 'log')
        os.makedirs(logdir)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

    try:
        print args
        if os.path.isfile(args.wrkdir + '/output/finished.txt'):
            os.remove(args.wrkdir + '/output/finished.txt')
        files = glob.glob(args.wrkdir + '/input/*.csv')
        if len(files) <> int(args.filecnt):
            raise Exception('ERROR: Input File Count Mismatch, expect: ' + str(args.filecnt) + ' actual: ' + str(len(files)))
        for xfile in files:
            logging.debug('Processing file: ' + str(xfile))
            process(xfile, kvals, outdir, logdir)  #args.kvals
            #os.remove(xfile)
        fin = Output(args.wrkdir + '/output/finished.txt')
        fin.write('finished at: ' + datetime.datetime.now().strftime('%H-%m-%d | %H:%M:%S'))
        fin.close()
    except Exception, e:
        logging.error('*****EXCEPTION*********')
        logging.error(str(e))

if __name__ == "__main__":
    main()
