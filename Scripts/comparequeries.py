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
                
def normalizeContent(lines, outLines):
    for line in lines:
        if not line.strip(): # Empty line.
            continue
        
        split = line.split(";")
        outSplit = []
        
        for elem in split:
            isTimestamp = re.match("\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}(:\d{2}[Z\s])?", elem)
            e2 = elem
            if isTimestamp:
                e2 = normalizeTimestamp(elem)
            outSplit.append(e2)
        
        first = True
        outLine = ""
        for elem in outSplit:
            if first:
                first = False
            else:
                outLine += ";"
            outLine += elem
        
        outLines.append(outLine)
    
def normalizeTimestamp(timestamp):
    # Influx includes 'Z' and 'T' in the output timestamp while Timescale doesn't, so a straight string-comparison would fail.
    ts = timestamp.replace("T", " ").replace("Z","")

    # A Java-LocalDateTime object that's been truncated to minutes doesn't print out its seconds-counter, so add it if it's missing.
    if ts.endswith(" 00:00"):
        ts = ts + ":00"
    
    return ts

def reportContentFail(id, v1, v2, f1, f2):
    print("Content mismatch:")
    print("  Results of query #" + str(id) + " doesn't fit across files.")
    print("  Values: '" + v1 + "', '" + v2 + "'")
    print("  First file: " + f1)
    print("  Second file: " + f2)
    print("  Skipping to next file.")
    
def reportContainsFail(id, line, f1, f2):
    print("Content mismatch: Line is in one file, but not the other.")
    print("  Results of query #" + str(id) + " doesn't fit across files.")
    print("  Line content: " + line)
    print("  First file: " + f1)
    print("  Second file: " + f2)
    print("  Skipping to next file.")
    
def reportLengthFail(id, v1, v2, f1, f2):
    print("Length mismatch")
    print("  Number of results in query #" + str(id) + " doesn't fit across files.")
    print("  Lengths: " + str(len(v1)) + ", " + str(len(v2)))
    print("  First file:  " + f1)
    print("  Second file: " + f2)

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
empty = 0
for i in range(num_elems):
    first_file = first_files[i]
    first_type = first_types[i]
    second_file = second_files[i]
    second_type = second_types[i]
    
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

    if len(first_lines) == 0 or len(second_lines) == 0:
        print("Both files are empty. May be expected or an error depending on query arguments.")
        print("  First file:  " + first_file)
        print("  Second file: " + second_file)
        empty += 1
        continue

    normal_first = []
    normal_second = []
    normalizeContent(first_lines, normal_first)
    normalizeContent(second_lines, normal_second)

    if first_type == "MaxForAP":
        # If lengths dont match, then the contents might still be equivalent because of differences between handling of absent APs for row- vs column schemas.
        # For the row-schema, absent APs can just not be added to the database, and querying for them returns e.g. an empty array
        # For the column-schema, absent APs need to be modelled as either a 0 or NULL (depending on database support for NULLs) and being able to tell that an AP
        #     was missing might therefore not be possible in the column-schema. Therefore, we want to consider dates with an AP with max-count of 0 the same as if it's not present.
        short = []
        long = []
        if len(normal_first) <= len(normal_second):
            short = normal_first
            long = normal_second
        else:
            short = normal_second
            long = normal_first
                
        lineSet = set()
        for line in long:
            lineSet.add(line)
        
        for line in short:
            if line in lineSet:
                lineSet.remove(line)
            else:
                reportContainsFail(i, line, first_file, second_file)
                break 
        
        if len(lineSet) == 0:
            successes += 1
        else:
            failure = False
            for line in lineSet:
                split = line.split(";")
                numClients = int(split[2])
                if numClients != 0:
                    reportContainsFail(i, line, first_file, second_file)                
                    failure = True
                    break
                
            if failure:
                fails += 1
            else:
                successes += 1
        
    elif first_type == "TotalClients" or first_type == "FloorTotals":
        if len(first_lines) != len(second_lines):
            reportLengthFail(i, first_lines, second_lines, first_file, second_file)
            fails += 1
            continue
    
        lineSet = set()
        for line in normal_first:
            lineSet.add(line)
        
        failure = False
        for line in normal_second:
            if not (line in lineSet):
                reportContainsFail(i, line, first_file, second_file)
                failure = True
                break

        if failure:
            fails += 1
        else:
            successes += 1
    elif first_type == "AvgOccupancy":
        # TODO: This might need the same handling as "Max For AP" for entries in one file being absent in the other.
        #       However, so far query-results haven't shown that to be needed.
    
        # No guarantee on ordering of lines in AvgOccupancy output.
        first_lines.sort()
        second_lines.sort()
        
        if len(first_lines) != len(second_lines):
            reportLengthFail(i, first_lines, second_lines, first_file, second_file)
            fails += 1
            continue
        
        failure = False
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
                failure = True
                break
            elif first_count != second_count:
                reportContentFail(i, first_count, second_count, first_file, second_file)
                failure = True
                break
            elif not floatComparer(first_now, second_now):
                reportContentFail(i, first_now, second_now, first_file, second_file)
                failure = True
                break
            elif not floatComparer(first_soon, second_soon):
                reportContentFail(i, first_soon, second_soon, first_file, second_file)
                failure = True
                break
        if failure:
            fails += 1
        else:
            successes += 1
    elif first_type == "KMeans":
        first_lines.sort()
        second_lines.sort()
        
        if len(first_lines) != len(second_lines):
            reportLengthFail(i, first_lines, second_lines, first_file, second_file)
            fails += 1
            continue
            
        failure = False
        for fl, sl in zip(first_lines, second_lines):
            if fl != sl:
                reportContentFail(i, fl, sl, first_file, second_file)
                print("  This may be expected if one execution uses the ROW-schema and the other the COLUMN-schema due to how missing values are handled in the K-Means implementation.")
                failure = True
                break
            
        if failure:
            fails += 1
        else:
            successes += 1
    else:
        # Something weird is wrong. Complain and exit.
        print("Unknown type: " + first_type)
        print("Exiting early")
        fails += 1
        break

print("")
print("Number of files: " + str(num_elems))
print("Successes: " + str(successes))
print("Failures: " + str(fails))
print("Empty: " + str(empty))
