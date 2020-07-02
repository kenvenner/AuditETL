'''
@author:   Ken Venner
@contact:  ken@venerllc.com
@version:  1.11

Library of tools used to read and write CSV files
'''

import csv
import kvmatch

# logging
import logging
logger = logging.getLogger(__name__)

# version number
AppVersion = '1.11'

# determine the max keys across a list of dictionaries
def max_column_list( csvlist ):
    fieldlist = []
    for rec in csvlist:
        for key in rec.keys():
            if key not in fieldlist:
                fieldlist.append(key)
    return fieldlist

# write out a list of dicts => record into a CSV
# based on one or more key fields
# assumes the first line of the CSV file is the header/defintion of the CSV
def writelist2csv( csvfile, csvlist, csvfields=None, mode='w', header=True, encoding='windows-1252', maxcolumns=False, debug=False ):
    # get the keys from the dictionary keys in the first value itself
    if not csvfields:
        csvfields = list( csvlist[0].keys() )

    # open the output file and write out the dictionary
    with open(csvfile, mode=mode, newline='', encoding=encoding) as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csvfields, extrasaction='ignore')

        if header:
            writer.writeheader()
        for row in csvlist:
            writer.writerow(row)


# write out a dict that is keyed => record into a CSV
# based on one or more key fields
# assumes the first line of the CSV file is the header/defintion of the CSV
def writedict2csv( csvfile, csvdict, csvfields=None, mode='w', header=True, encoding='windows-1252', maxcolumns=False, debug=False ):
    # get the keys from the dictionary keys in the first value itself
    if not csvfields:
        csvfields = list( csvdict[ list( csvdict.keys() )[0] ].keys() )

    # open the output file and write out the dictionary
    with open(csvfile, mode=mode, newline='', encoding=encoding) as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csvfields, extrasaction='ignore')

        if header:
            writer.writeheader()
        for row in csvdict.values():
            writer.writerow(row)


################################ READ #############################################

# read in the CSV and create a dictionary to the records
# based on one or more key fields
# assumes the first line of the CSV file is the header/defintion of the CSV
def readcsv2list_with_header( csvfile, headerlc=False, encoding='windows-1252', debug=False ):
    results = []
    with open(csvfile, mode='r', encoding=encoding) as csv_file:
        reader = csv.reader(csv_file)
        header = reader.__next__()
        if debug: print('header-before:', header)
        logger.debug('header-before:%s', header)
        if headerlc:
            header = [x.lower() for x in header]
            if debug: print('header-after:', header)
            logger.debug('header-after:%s', header)
        for row in reader:
            rowdict = dict(zip(header,row))
            # create/update the dictionary
            results.append(rowdict)
    # return the results
    return results, header

# read in the CSV and create a dictionary to the records
# based on one or more key fields
# assumes the first line of the CSV file is the header/defintion of the CSV
def readcsv2list( csvfile, headerlc=False, encoding='windows-1252', debug=False ):
    results, header = readcsv2list_with_header( csvfile, headerlc, encoding, debug )
    return results


            
# read in the CSV and create a dictionary to the records
# based on one or more key fields
# assumes the first line of the CSV file is the header/defintion of the CSV
def readcsv2dict_with_header( csvfile, dictkeys, dupkeyfail=False, noshowwarning=False, headerlc=False, encoding='windows-1252', debug=False ):
    results  = {}
    dupkeys  = []
    dupcount = 0
    with open(csvfile, mode='r', encoding=encoding) as csv_file:
        reader = csv.reader(csv_file)
        header = reader.__next__()
        if debug: print('header-before:', header)
        logger.debug('header-before:%s', header)
        if headerlc:
            dictkeys = [x.lower() for x in dictkeys]
            header = [x.lower() for x in header]
            if debug: print('header-after:', header)
            logger.debug('header-after:%s', header)
        for row in reader:
            rowdict = dict(zip(header,row))
            reckey = kvmatch.build_multifield_key(rowdict, dictkeys)
            # do we fail if we see the same key multiple times?
            if reckey in results:
                dupcount += 1
                if dupkeyfail:
                    # capture this key
                    dupkeys.append(reckey)
            # create/update the dictionary
            results[reckey] = rowdict
    # fail if we found dupkeys
    if dupkeys:
        # log this issue
        logger.warning('readcsv2dict:v%s:file:%s:duplicate key failure:keys:%s',AppVersion, csvfile,','.join(dupkeys) )
        # display message if the user wants this displayed
        if not noshowwarning:
            print('readcsv2dict:duplicate key failure:', ','.join(dupkeys))
        raise ValueError('Duplicate key failure')
    # return the results
    return results, header, dupcount


# read in the CSV and create a dictionary to the records
# based on one or more key fields
# assumes the first line of the CSV file is the header/defintion of the CSV
def readcsv2dict( csvfile, dictkeys, dupkeyfail=False, noshowwarning=False, headerlc=False, encoding='windows-1252', debug=False ):
    results, header, dupcnt = readcsv2dict_with_header( csvfile, dictkeys, dupkeyfail=dupkeyfail, noshowwarning=noshowwarning, headerlc=headerlc, encoding=encoding, debug=debug )
    return results






# read in the CSV and create a dictionary to the records
# based on one or more key fields
# no header on this file - the header must be passed in
def readcsv2dict_with_noheader( csvfile, dictkeys, header, dupkeyfail=False, noshowwarning=False, encoding='windows-1252', debug=False ):
    if not dictkeys:
        logger.error('must pass in dictkeys')
        raise Exception('must pass in dictkeys')
    if not header:
        logger.error('must pass in header')
        raise Exception('must pass in header')
    if not isinstance(header,list):
        logger.error('header must be a list:%s',header)
        raise Exception('header must be a list:%s',header)

    
    results  = {}
    dupkeys  = []
    dupcount = 0
    with open(csvfile, mode='r', encoding=encoding) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            rowdict = dict(zip(header,row))
            reckey = kvmatch.build_multifield_key(rowdict, dictkeys)
            # do we fail if we see the same key multiple times?
            if reckey in results:
                dupcount += 1
                if dupkeyfail:
                    # capture this key
                    dupkeys.append(reckey)
            # create/update the dictionary
            results[reckey] = rowdict
    # fail if we found dupkeys
    if dupkeys:
        # log this issue
        logger.warning('readcsv2dict:v%s:file:%s:duplicate key failure:keys:%s',AppVersion, csvfile,','.join(dupkeys) )
        # display message if the user wants this displayed
        if not noshowwarning:
            print('readcsv2dict:duplicate key failure:', ','.join(dupkeys))
        raise ValueError('Duplicate key failure')
    # return the results
    return results, header, dupcount



################ FINDHEADER ############################

# coding structure - build one generic (INTERNAL) function that does all the various things
# with passed in variables that are all optional
# and based on variable settings - executes the behavior being asked
#
# then create external functions - with clear passed in parameters that calls this internal function with teh right settings
#

# read in the CSV and create a dictionary to the records
# based on one or more key fields
# assumes the first line of the CSV file is the header/defintion of the CSV
# features to add
#
#   noheader - flag means we pass in the array that is the header
#   col_header - flag means we get the header from the very first line (or start_row) in the file
#   aref_result - flag that tells us to return the array of array (otherwise we pass back the array of dict)
#
#   col_aref - array that defines the columns we read into dict
#
#   start_row - user entered values start at row 1 (not zero)
#
#   unique_column - if enabled, we must validate that our column names are unique
#   ignore_blank_row - if enabled, if the row read has no data - we don't put the data into the extracted list
#   ignore_not_fill - not sure what this one is
#
#   required_fields_populated - checking logic to assure that all required fields have data with optional
#     required_fld_swap - a dict that says if key is not populated - check the value tied to that key to see if it is populated
#

def readcsv2list_findheader( csvfile, req_cols, xlatdict={}, optiondict={}, col_aref=None, debug=False ):

    # local variables
    results = []
    header = None
    
    # debugging
    if debug: print('req_cols:', req_cols)
    if debug: print('xlatdict:', xlatdict)
    if debug: print('optiondict:', optiondict)
    if debug: print('col_aref:', col_aref)
    logger.debug('req_cols:%s', req_cols)
    logger.debug('xlatdict:%s', xlatdict)
    logger.debug('optiondict:%s', optiondict)
    logger.debug('col_aref:%s', col_aref)

    # check type
    if col_aref and not isinstance(col_aref,list):
        logger.error('col_aref must be list:%s', col_aref)
        raise Exception('col_aref not a list')
    
    # set flags
    col_header  = False  # if true - we take the first row of the file as the header
    no_header   = False  # if true - there are no headers read - we either return 
    aref_result = False  # if true - we don't return dicts, we return a list
    save_row    = False  # if true - then we append/save the XLSRow with the record
    
    start_row   = 0      # if passed in - we start the search at this row (starts at 1 or greater)

    # create the list of misconfigured solutions
    badoptiondict = {
        'startrow'       : 'start_row',
        'startrows'      : 'start_row',
        'start_rows'     : 'start_row',
        'colheaders'     : 'col_header',
        'col_headers'    : 'col_header',
        'noheader'       : 'no_header',
        'noheaders'      : 'no_header',
        'no_headers'     : 'no_header',
        'arefresult'     : 'aref_result',
        'arefresults'    : 'aref_result',
        'aref_results'   : 'aref_result',
        'saverow'        : 'save_row',
        'saverows'       : 'save_row',
        'save_rows'      : 'save_row',
    }

    # check what got passed in
    kvmatch.badoptiondict_check( 'kvcsv.readcsv2list_findheader', optiondict, badoptiondict, True )
        
    
    # pull in passed values from optiondict
    if 'col_header'  in optiondict: col_header = optiondict['col_header']
    if 'aref_result' in optiondict: aref_result = optiondict['aref_result']
    if 'no_header'   in optiondict: no_header = optiondict['no_header']
    if 'start_row'   in optiondict: start_row = optiondict['start_row'] - 1 # because we are not ZERO based in the users mind
    if 'save_row'    in optiondict: save_row = optiondict['save_row']
    

    # build object that will be used for record matching
    p = kvmatch.MatchRow( req_cols, xlatdict, optiondict )

    # get the file opened
    csv_file = open(csvfile, mode='r')
    reader = csv.reader(csv_file)
    
    # ------------------------------- HEADER START ------------------------------

    
    # define the header for the records being read in
    if no_header:
        # user said we are not to look for the header in this file
        # we need to subtract 1 here because we are going to increment PAST the header
        # in the next section - so if there is no header - we need to start at zero ( -1 + 1 later)
        row_header = start_row - 1

        # if no col_aref - then we must force this to aref_result
        if not col_aref:
            aref_result = True
            if debug:  print('no_header:no col_aref:set aref_result to true')
            logger.debug('no_header:no col_aref:set aref_result to true')
            
        # debug
        if debug:  print('no_header:start_row:', start_row)
        logger.debug('no_header:start_row:%d', start_row)
        
    elif col_header:
        # extract the header as the first line in the file
        header = reader.__next__()
        row_header = 0 
        if debug: print('col_header:header_1strow:',header)
        logger.debug('col_header:header_1strow:%s',header)
    else:
        # debug
        if debug: print('find_header:start_row:', start_row)
        logger.debug('find_header:start_row:%d', start_row)
        
        # get to the start_row record
        for next_row in range(0,start_row):
            line = reader.__next__()
            if debug: print('skipping line:', line)
            logger.debug('skipping line:%s', line)

        # counting row just to provide feedback
        row = start_row
        
        # now start the search for the header
        for rowdata in reader:
            # increment row
            row += 1
            
            # have not found the header yet - so look
            if debug:  print('looking for header at row:', row)
            logger.debug('looking for header at row:%d', row)

            # Search to see if this row is the header
            if p.matchRowList( rowdata, debug=debug ) or p.search_exceeded:
                # determine if we found the header
                # set the row_header
                row_header = row
                # found the header grab the output
                header = p._data_mapped
                # debugging
                if debug: print('header_found:',header)
                logger.debug('header_found:%s',header)
                # break out of the loop
                break
            elif p.search_exceeded:
                # close the file we opened
                csv_file.close()
                # did not find the header - raise error
                raise Exception('header not found')


    # ------------------------------- HEADER END ------------------------------

    # debug
    if debug:  print('exitted header find loop')
    logger.debug('exitted header find loop')
    
    # user wants to define/override the column headers rather than read them in
    if col_aref:
        # debugging
        if debug:  print('copying col_aref into header')
        logger.debug('copying col_aref into header')
        # copy over the values - and determine if we need to fill in more header values
        header = col_aref[:]
        # user defined the row definiton - make sure they passed in enough values to fill the row
        sheetmaxcol = 0
        sheetmincol = 0
        if False:
            if len(col_aref) < sheetmaxcol - sheetmincol:
                # not enough entries - so we add more to the end
                for colcnt in range(1, sheetmaxcol - sheetmincol - len(col_aref) + 1 ):
                    header.append('')

        # now pass the final information through remapped
        header = p.remappedRow(header)
        # debug
        if debug: print('col_aref:header:', header)
        logger.debug('col_aref:header:%s', header)

    # ------------------------------- RECORDS START ------------------------------

    # continue processing this file
    for rowdata in reader:

        if debug: print('rowdata:', rowdata)
        logger.debug('rowdata:%s', rowdata)

        # determine what we are returning
        if aref_result:

            # we want to return the data we read
            rowdict = rowdata
            if debug:  print('saving as array')
            logger.debug('saving as array')
            
            # optionally add the XLSRow attribute to this dictionary (not here right now
            if save_row:
                rowdict.append( row + 1 )
                if debug: print('append row to record')
                logger.debug('append row to record')

        else:
            # we found the header so now build up the records
            rowdict = dict(zip(header,rowdata))
            if debug:  print('saving as dict')
            logger.debug('saving as dict')

            # optionally add the XLSRow attribute to this dictionary (not here right now
            if save_row:
                rowdict['XLSRow'] = row + 1
                if debug: print('add column XLSRow with row to record')
                logger.debug('add column XLSRow with row to record')
                
        # add this dictionary to the results
        results.append(rowdict)
        if debug:  print('append rowdict to results')
        logger.debug('append rowdict to results')

    # ------------------------------- RECORDS END ------------------------------

    # close the file we are reading
    csv_file.close()
    
    # debugging
    # if debug: print('results:', results)
    
    # return the results
    return results


def readcsv2dict_findheader( csvfile, req_cols, dictkeys, xlatdict={}, optiondict={}, col_aref=None, debug=False, dupkeyfail=False ):

    # check inputs
    if not dictkeys:
        raise Exception('dictkeys must be populated')
    elif not isinstance(dictkeys,list):
        raise Exception('dictkeys must be a list:%s', dictkeys)

    # debugging
    if debug:
        print('dictkeys:', dictkeys)
        
    # local variables
    dupkeys = []
    dictresults={}

    # read in the data from the file
    results = readcsv2list_findheader( csvfile, req_cols, xlatdict=xlatdict, optiondict=optiondict, col_aref=col_aref, debug=debug )

    # check processing
    if 'no_header' in optiondict and optiondict['no_header'] and not col_aref:
        raise Exception('invalid setting optiondict[no_header] and no col_aref')
    if 'aref_result' in optiondict and optiondict['aref_result']:
        raise Exception('invalid setting optiondict[aref_result]')
    
    if debug:
        print('results:', results)
        
    # convert to a dictionary based on keys provided
    for rowdict in results:
        if debug:
            print('dictkeys:', dictkeys)
            print('rowdict:', rowdict)
            
        reckey = kvmatch.build_multifield_key(rowdict, dictkeys)
        # do we fail if we see the same key multiple times?
        if dupkeyfail:
            if reckey in dictresults:
                # capture this key
                dupkeys.append(reckey)

        # create/update the dictionary
        dictresults[reckey] = rowdict

    # fail if we found dupkeys
    if dupkeys:
        print('readcsv2dict:duplicate key failure:', ','.join(dupkeys))
        raise Exception('duplicate key failure:%s', dupkeys)

    if debug:
        print('dictresults:', dictresults)
        
    # return the results
    return dictresults




if __name__ == '__main__':

    inputfile = 'wine_xlat.csv'
    inputkeys = ['Company','Wine']
    outputfile = 'wine_xlat_test.csv'
    
    results =  readcsv2dict( inputfile, inputkeys )
    #print( results )
    [print(row, ',') for row in results.values()]

    writedict2csv( outputfile, results )
    print( 'review file:', outputfile)

#eof
