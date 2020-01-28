import argparse
import os
import re

parser = argparse.ArgumentParser(description='Compare query-results from executing the benchmark against different databases.')
parser.add_argument('first', help='A path to the folder containing saved query-results')
parser.add_argument('second', help='A path to the folder containing saved query-results')
args = vars(parser.parse_args())

first_path = args['first']
second_path = args['second']

first_files = {}
first_types = {}
second_files = {}
second_types = {}

def get_files(path, fileOut, typeOut):
    for r,d,f in os.walk(path):
        for file in f:
            if '.csv' in file: # Skip the file containing the config that we also write to the dir, for convenience sake.
                contents = re.split('[_.]', file)
                id = int(contents[0])
                queryType = contents[1]
                fileOut[id] = os.path.join(r,file)
                typeOut[id] = queryType
                
def compareTimestamps(ts1, ts2):
    #TODO: Actually parse the timestamp?
    ts1 = normalizeTimestamp(ts1)
    ts2 = normalizeTimestamp(ts2)
    
    return ts1 == ts2
    
def normalizeTimestamp(timestamp):
    # Influx includes 'Z' and 'T' in the output timestamp while Timescale doesn't, so a straight string-comparison would fail.
    return timestamp.replace("T", " ").replace("Z","")
    
def timestampSort(line, ts_index):
    if line == "":
        return ""
    linecontents = line.split(";")
    return normalizeTimestamp(linecontents[ts_index])

def reportContentFail(id, v1, v2, f1, f2):
    print("Content mismatch:")
    print("  Results of query #" + str(id) + " doesn't fit across files.")
    print("  Values: '" + v1 + "', '" + v2 + "'")
    print("  First file: " + f1)
    print("  Second file: " + f2)
    print("  Skipping to next file.")

def floatComparer(v1, v2):
    # Averaging in parallel on db using sql versus sequentially in Java produces values that seem to vary by up to this threshold given the same input...
    threshold = 1.5
    
    f1 = float(v1)
    f2 = float(v2)
    return (f1 + threshold) > f2 and (f1 - threshold) < f2

get_files(first_path, first_files, first_types)
get_files(second_path, second_files, second_types)

# More queries might have been executed against one database than against the other.
# We just want to compare those that have been run against both, so throw away any extras.
first_max_id = max(first_files.keys())
second_max_id = max(second_files.keys())

if first_max_id != second_max_id:
    print("First has " + str(first_max_id) + " results while second has " + str(second_max_id) + " results.")
    print("Only the initial " + str(first_max_id if first_max_id < second_max_id else second_max_id) + " results will be compared.")

if first_max_id > second_max_id:
    for i in range(second_max_id+1, first_max_id+1):
        del first_files[i]
        del first_types[i]
elif first_max_id < second_max_id:
    for i in range(first_max_id+1, second_max_id+1):
        del second_files[i]
        del second_types[i]
    
num_elems = len(first_files)

successes = 0
fails = 0
for i in range(num_elems):
    first_file = first_files[i]
    first_type = first_types[i]
    second_file = second_files[i]
    second_type = second_types[i]
    success = False
    
    if first_type != second_type:
        # Something weird is wrong. Complain and exit.
        print("Type mismatch")
        print("  Likely caused by non-comparable configs (bad seed? different query-probabilities?)")
        print("  First was " + first_type + ", second was " + second_type)
        print("  First path:  " + first_file)
        print("  Second path: " + second_file)
        print("Exiting early")
        fails += 1
        break

    first_lines = [line.rstrip('\n') for line in open(first_file)]
    second_lines = [line.rstrip('\n') for line in open(second_file)]

    if len(first_lines) != len(second_lines):
        print("Length mismatch")
        print("  Number of results in query #" + str(i) + " doesn't fit across files.")
        print("  Lengths: " + str(len(first_lines)) + ", " + str(len(second_lines)))
        print("  First file:  " + first_file)
        print("  Second file: " + second_file)
        fails += 1
        continue
        
    if len(first_lines) == 0 or len(second_lines) == 0:
        print("Note: Both files are empty. Counting as success, but may be a widespread error if this happens often.")
        print("  First file:  " + first_file)
        print("  Second file: " + second_file)
        successes += 1
        continue

    # TODO: Refactor these if/elif statements, if I can be bothered...
    if first_type == "MaxForAP":
        # No guarantee on ordering of lines in MaxForAP output.
        # Sort by the timestamp
        first_lines.sort(key=lambda x: timestampSort(x, 1))
        second_lines.sort(key=lambda x: timestampSort(x, 1))
        
        for fl, sl in zip(first_lines, second_lines):
            first_content = fl.split(";")
            second_content = sl.split(";")
            
            first_AP = first_content[0]
            first_timestamp = first_content[1]
            first_maxval = first_content[2]
            
            second_AP = second_content[0]
            second_timestamp = second_content[1]
            second_maxval = second_content[2]
            
            if first_AP != second_AP:
                reportContentFail(i, first_AP, second_AP, first_file, second_file)
                break
            elif not compareTimestamps(first_timestamp, second_timestamp):      
                reportContentFail(i, first_timestamp, second_timestamp, first_file, second_file)
                break
            elif first_maxval != second_maxval:
                reportContentFail(i, first_maxval, second_maxval, first_file, second_file)
                break
            else:
                success = True
    elif first_type == "TotalClients":
        # No guarantee on ordering of lines in TotalClients output.
        # Sort by the timestamp
        first_lines.sort(key=lambda x: timestampSort(x, 0))
        second_lines.sort(key=lambda x: timestampSort(x, 0))
        
        for fl, sl in zip(first_lines, second_lines):
            first_content = fl.split(";")
            second_content = sl.split(";")
            
            first_timestamp = first_content[0]
            first_total = first_content[1]
            
            second_timestamp = second_content[0]
            second_total = second_content[1]
            
            if not compareTimestamps(first_timestamp, second_timestamp):      
                reportContentFail(i, first_timestamp, second_timestamp, first_file, second_file)
                break
            elif first_total != second_total:
                reportContentFail(i, first_total, second_total, first_file, second_file)
                break
            else:
                success = True
    elif first_type == "FloorTotals":
        # No guarantee on ordering of lines in FloorTotals output.
        # Sort is stable, so sort on floor-number first, then on timestamp
        first_lines.sort( key=lambda x: "" if x == "" else x.split(";")[1])
        second_lines.sort(key=lambda x: "" if x == "" else x.split(";")[1])
        first_lines.sort( key=lambda x: timestampSort(x, 0))
        second_lines.sort(key=lambda x: timestampSort(x, 0))
        
        for fl, sl in zip(first_lines, second_lines):
            first_content = fl.split(";")
            second_content = sl.split(";")
            
            first_timestamp = first_content[0]
            first_floor = first_content[1]
            first_total = first_content[2]
            
            second_timestamp = second_content[0]
            second_floor = second_content[1]
            second_total = second_content[2]
            
            if not compareTimestamps(first_timestamp, second_timestamp):      
                reportContentFail(i, first_timestamp, second_timestamp, first_file, second_file)
                break
            elif first_floor != second_floor:
                reportContentFail(i, first_floor, second_floor, first_file, second_file)
                break
            elif first_total != second_total:
                reportContentFail(i, first_total, second_total, first_file, second_file)
                break
            else:
                success = True
    elif first_type == "AvgOccupancy":
        # No guarantee on ordering of lines in AvgOccupancy output.
        # Sort on AP name
        first_lines.sort()
        second_lines.sort()
        
        for fl, sl in zip(first_lines, second_lines):
            first_content = fl.split(";")
            second_content = sl.split(";")
            
            first_AP = first_content[0]
            first_count = first_content[1]
            first_now = first_content[2]
            first_soon = first_content[3]
            
            second_AP = second_content[0]
            second_count = second_content[1]
            second_now = second_content[2]
            second_soon = second_content[3]
            
            if first_AP != second_AP:
                reportContentFail(i, first_AP, second_AP, first_file, second_file)
                break
            elif first_count != second_count:
                reportContentFail(i, first_count, second_count, first_file, second_file)
                break
            elif not floatComparer(first_now, second_now):
                reportContentFail(i, first_now, second_now, first_file, second_file)
                break
            elif not floatComparer(first_soon, second_soon):
                reportContentFail(i, first_soon, second_soon, first_file, second_file)
                break
            else:
                success = True
    else:
        # Something weird is wrong. Complain and exit.
        print("Unknown type: " + first_type)
        print("Exiting early")
        fails += 1
        break
    
    if(success):
        successes += 1
    else:
        fails += 1

print("")
print("Number of files: " + str(num_elems))
print("Successes: " + str(successes))
print("Failures: " + str(fails))
