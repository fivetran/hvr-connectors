#!python

################################################################################
# 
# NAME
#     hvrsinkagent.py
#
# SYNOPSIS
#     as agent
#     python hvrsinkagent.py mode loc chn agrs
#   
# DESCRIPTION
#
#     This script can be used to send replicated target files to the 
#     bit bucket
#
# ENVIRONMENT VARIABLES
#     HVR_BB_TRACE 
#
#     HVR_FILE_LOC          (required)
#        This varibale sets a location of files to be sent
#  
#     HVR_FILE_NAMES        (required)
#        This  variable contains list of files processed by integrate or refresh
#        If empty - intergarion process is omitted
#
################################################################################
import sys
import traceback
import os

trace_level = 0
hvr_file_loc = ''
hvr_files = []

##### Support function #########################################################

def trace(level, msg):
    if trace_level >= level:
        print(msg)
        sys.stdout.flush() 

def version_normalizer(version):
    version_array = version.split('.')
    version_num = 0
    least_significant = len(version_array) - 1
    if least_significant > 2:
        least_significant = 2
    shift = 1
    for i in range(least_significant, -1, -1):
        version_num += int(version_array[i]) * shift
        shift *= 1000
    return version_num
    
def version_check():
    global python3
    python3 = sys.version_info[0] == 3
    
def env_load():
    global trace_level
    global hvr_file_loc
    global hvr_files

    trace_level = int(os.getenv('HVR_BB_TRACE', '0'))
    hvr_file_loc = os.getenv('HVR_FILE_LOC', '')
    hvr_files = os.getenv('HVR_FILE_NAMES', '').split(":")

def print_agent_input(argv):
    if trace_level < 1:
        return
    """
    """
    print("============================================")
    for i in range(1, len(argv)):
        print("arg[{0}] = {1}".format(i, argv[i]))
    print("============================================")
    env = os.environ
    if python3:
        for key, value  in env.items():
            if key.find('HVR') != -1:
                print(key+ " = " + value)
    else:
        for key, value  in env.iteritems():
            if key.find('HVR') != -1:
                print(key+ " = " + value)
    print("============================================")

def file_loc_cleanup():
    global file_count
    file_count = 0
    for name in hvr_files:
        if not name:
            continue
        full_name = os.path.join(hvr_file_loc, name)
        if not os.path.exists(full_name):
            continue
        trace(1, "Remove {}".format(full_name))
        file_count += 1
        os.remove(full_name)   

def process(argv):
    version_check()
    env_load()
    print_agent_input(argv)

    if argv[1] == "refr_write_end" or argv[1] == "integ_end":
        file_loc_cleanup()
        print("{0} files removed".format(file_count))

if __name__ == "__main__":
    try:
        process(sys.argv)
        sys.stdout.flush() 
        sys.exit(0) 
    except Exception as err:
        sys.stdout.flush() 
        sys.stderr.write("F_JX0D03: {0}\n".format(err))
        sys.stderr.flush()
        sys.exit(1);

