'''
@author:   Ken Venner
@contact:  ken@venerllc.com
@version:  1.01

This tool reads EShare Audit log CSV files and performs a number
of analytic analysis on the file

The longer range goal is to add ETL logic into this tool to take 
the CSV file and perform field renames and perform any transform logic
so that it generates the expected / standard inputs to the 
analytics DBMS that will load this xformed data.

'''

# import libraries that are being used
import kvutil
import kvcsv

import sys

# may comment out in the future
import pprint
pp = pprint.PrettyPrinter(indent=4)
ppFlag = False


# logging
import sys
import kvlogger
# pick the log file structure from list below
# single file that is rotated
config=kvlogger.get_config(kvutil.filename_create(__file__, filename_ext='log', path_blank=True), loggerlevel='INFO') #single file
# one file per day of month
# config=kvlogger.get_config(kvutil.filename_log_day_of_month(__file__, ext_override='log'), 'logging.FileHandler') # one file per day of month
kvlogger.dictConfig(config)
logger=kvlogger.getLogger(__name__)

# added logging feature to capture and log unhandled exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        # if this is a keyboard interrupt - we dont' want to handle it here
        # reset and return
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    # other wise catch/log this error
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

# create overall hook to catch uncaught exceptions
sys.excepthook = handle_exception



# application variables
optiondictconfig = {
    'AppVersion' : {
        'value' : '1.01',
        'description' : 'defines the version number for the app',
    },
    'input_file' : {
        'value' : None,
        'description' : 'defines the audit file we are parsing and processing',
    },
    'input_list' : {
        'value' : None,
        'type'  : 'liststr',
        'description' : 'defines the comma separated list of audit file we are parsing and processing',
    },
    'input_glob' : {
        'value' : None,
        'description' : 'defines the file glob used to define the list of records to be read in and processed',
    },
    'eventfldfmt' : {
        'value' : 'Event_Items_{}_{}',
        'description' : 'defines the format string used to construct the columns that tie to events',
    },
    'disp_all_flds' : {
        'value' : False,
        'type'  : 'bool',
        'description' : 'flag controlling output of fldPopulate counts by field across all records',
    },
    'disp_all_always' : {
        'value' : False,
        'type'  : 'bool',
        'description' : 'flag controlling output of fields populated across all records',
    },
    'disp_action_always' : {
        'value' : None,
        'type'  : 'inlist',
        'valid' : ['csv','pivot',None],
        'description' : 'flag controlling output of fldPopulate by action_name unique field data and the format of the output',
    },
    'disp_action_sometimes' : {
        'value' : None,
        'type'  : 'inlist',
        'valid' : ['csv','pivot',None],
        'description' : 'flag controlling output of fldPopulate by action_name unique field data and the format of the output',
    },
    'disp_event_split' : {
        'value' : False,
        'type'  : 'bool',
        'description' : 'flag controlling output of collapse of Event_Item_XX_XX into discrete records',
    },
}


#### put local functions here #####
def flatten_event_items( csvevent, eventfldfmt, fldNumMap=['file_path','file_name'], maxEventCnt=400 ):
    filelist = []
    for eventcnt in range(maxEventCnt):
        rec = {}
        for fldcnt in range(len(fldNumMap)):
            eventfld = eventfldfmt.format(eventcnt,fldcnt)
            if eventfld not in header or not csvevent[eventfld]:
                break
            rec[fldNumMap[fldcnt]] = csvevent[eventfld]

        if not rec:
            break
        filelist.append(rec)
    return filelist

def cnt_field_filled_by_action_name( csvfile ):
    header = csvfile[0].keys()

    # get the unique list of actino names
    action_name = []
    for rec in csvfile:
        if rec['Action_Name'] not in action_name:
            action_name.append(rec['Action_Name'])
            
    # the math works when I build via brute force.
    fldPopulated = {}
    for aname in action_name:
        fldPopulated[aname] = {}
        for fld in header:
            fldPopulated[aname][fld] = { 'cnt_in' : 0, 'cnt_notin' : 0 }

    for csvevent in csvfile:
        aname = csvevent['Action_Name']
        for fld in header:
            if csvevent[fld]:
                updtfld = 'cnt_in'
            else:
                updtfld = 'cnt_notin'
            fldPopulated[aname][fld][updtfld] +=1

    return fldPopulated

def cnt_field_filled( csvfile ):
    header = csvfile[0].keys()

    # something weird happens when I build the dict this way - not sure why
    #fldPopulated = dict.fromkeys( header, { 'cnt_in' : 0, 'cnt_notin' : 0 } )

    # the math works when I build via brute force.
    fldPopulated = {}
    for fld in header:
        fldPopulated[fld] = { 'cnt_in' : 0, 'cnt_notin' : 0 }

    for csvevent in csvfile:
        for fld in header:
            if csvevent[fld]:
                updtfld = 'cnt_in'
            else:
                updtfld = 'cnt_notin'
            fldPopulated[fld][updtfld] +=1

    return fldPopulated

def field_all_or_not( fldPopulated ):
    all = []
    some = {}
    for key,val in fldPopulated.items():
        if val['cnt_notin']:
            some[key] = val
        else:
            all.append(key)

    return all,some


def merge_dict_with_cnt( master_dict, new_dict, twolevel=False ):
    for key,val in new_dict.items():
        if key not in master_dict:
            master_dict[key] = val
        else:
            for key2,val2 in val.items():
                if key2 not in new_dict:
                    master_dict[key][key2] = val2
                else:
                    if twolevel:
                        for key3,val3 in val2.items():
                            master_dict[key][key2][key3] += val3
                    else:
                        master_dict[key][key2] += val2
                
                

# ---------------------------------------------------------------------------
if __name__ == '__main__':

    # capture the command line
    # optional setttings in code
    #   raise_error = True - if we have a problem parsing option we raise an error rather than pass silently
    #   keymapdict = {} - dictionary of mis-spelling of command options that are corrected for through this mapping
    #   debug = True - provide insight to what is going on as we parse conf_json files and command line options
    optiondict = kvutil.kv_parse_command_line( optiondictconfig, debug=False )

    # get the list of files to process
    csvfilelist = kvutil.filename_list( optiondict['input_file'],optiondict['input_list'], optiondict['input_glob'] )

    if not csvfilelist:
        print('Must specify at least one file to process in:  input_file, input_list or input_glob')
        sys.exit(1)

    # now process these files
    fldPopulated = {}
    fldPopulatedByActionName = {}
    for csvfilename in csvfilelist:
        # read in the content of the file
        csvlist,header = kvcsv.readcsv2list_with_header( csvfilename )

        # find out what fields are always populated across this one file
        fldPopulatedFile = cnt_field_filled( csvlist )

        # similar analysis but by action_name - what fields are always populated
        fldPopulatedByActionNameFile = cnt_field_filled_by_action_name( csvlist )

        # merge these results
        merge_dict_with_cnt( fldPopulated, fldPopulatedFile )
        merge_dict_with_cnt( fldPopulatedByActionName, fldPopulatedByActionNameFile, True )


    # separate into fields that are always populated and some times populated
    all,some = field_all_or_not( fldPopulated )

    # optionall print out data
    if optiondict['disp_all_flds']:
        print('fldPopulated:')
        pp.pprint(fldPopulated)

    if optiondict['disp_all_always']:
        print('Always populated:')
        pp.pprint(all)


    if optiondict['disp_action_always']:
        if optiondict['disp_action_always'] != 'csv':
            print('Action_Name,AlwaysFld')
        for aname in fldPopulatedByActionName.keys():
            bynameall,bynamesome = field_all_or_not( fldPopulatedByActionName[aname] )
            alluniq = [i for i in bynameall if i not in all]
            if optiondict['disp_action_always'] == 'csv':
                print('{},{}'.format(aname,','.join(alluniq)))
            else:
                [print('{},{}'.format(aname,i)) for i in alluniq]
        
    if optiondict['disp_action_sometimes']:
        if optiondict['disp_action_sometimes'] != 'csv':
            print('Action_Name,SometimesFld,Percent')
        for aname in fldPopulatedByActionName.keys():
            bynameall,bynamesome = field_all_or_not( fldPopulatedByActionName[aname] )
            someuniq = {i:j for i,j in bynamesome.items() if j['cnt_in'] > 0}
            if optiondict['disp_action_sometimes'] == 'csv':
                print('{},{}'.format(aname,','.join(someuniq.keys())))
            else:
                [print('{},{},{}'.format(aname,i,someuniq[i]['cnt_in']/(someuniq[i]['cnt_in']+someuniq[i]['cnt_notin']))) for i in someuniq.keys()]
        
    # determine if we any event fields to deal with
    eventfld0 = optiondict['eventfldfmt'].format(0,0)

    # determine if we are going to run this at all 
    if eventfld0 in header and optiondict['disp_event_split']:
        actionNameWithEvent = {}
        for csvevent in csvfile:
            if csvevent['Action_Name'] not in actionNameWithEvent:
                filelist = flatten_event_items( csvevent, optiondict['eventfldfmt'] )
                if filelist:
                    actionNameWithEvent[csvevent['Action_Name']] = filelist

        if True:
            csvreccnt=0
            print('csvfile[{}]'.format(csvreccnt))
            pp.pprint(csvfile[csvreccnt])
            print('filelist:')
            pp.pprint(filelist)

            pp.pprint(actionNameWithEvent)

#eof
