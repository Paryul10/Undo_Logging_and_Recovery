import os
import sys
import re
import csv
import operator
import copy

DEBUG_FLAG = 0
disk_variables = {}
memory_variables = {}
end_flag = 0
undo_log = []
completed_transactions = []

def print_variables():
    j = 0
    lens = len(disk_variables.keys())
    for i in sorted (disk_variables.keys()): 
        if j == lens - 1:
            print(i,disk_variables[i], end = "")
        else:
            print(i,disk_variables[i], end = " ")
        j += 1
    print()

def read_and_parse_data(filename):

    try:
        with open(filename, "r") as file:
            lines=file.readlines()
            tot_lines=len(lines)

            i=0
            while i < tot_lines:
                lines[i]=lines[i].strip()
                i += 1
            
            if DEBUG_FLAG:
                print(lines)
            
            i=0
            while i < tot_lines:
                if DEBUG_FLAG == 1:
                    print(lines[i])
                if lines[i] != '':
                    if i == 0:
                        ## read the variables into disk
                        line = lines[i].split(' ')
                        line_len = len(line)
                        for j in range(0,line_len,2):
                            disk_variables[line[j]] = int(line[j+1])
                    else:
                        temp = []
                        lines[i] = lines[i].strip('<>')
                        if lines[i].find("START") != -1:
                            if lines[i].find("CKPT") == -1:
                                lines[i] = lines[i].split(" ")
                            else:
                                lines[i] = lines[i].split(" ")
                                temp.append(lines[i][0])
                                temp.append(lines[i][1])
                                lens = len(lines[i])
                                for j in range(2,lens):
                                    temp.append(lines[i][j].strip('()').strip(','))
                                lines[i] = temp
                                
                        elif lines[i].find("COMMIT") != -1:
                            lines[i] = lines[i].split(" ")
                        elif lines[i].find("END") != -1:
                            lines[i].split(" ")
                        else:
                            lines[i] = lines[i].split(", ")                                                        
                        undo_log.append(lines[i])
                else:
                    pass
                        
                i += 1

            if DEBUG_FLAG == 1:
                print(disk_variables)

            if DEBUG_FLAG == 1:
                print(undo_log)

            
            
            undo_log.reverse()
            
            if DEBUG_FLAG == 1:
                print("ulgs",undo_log)
            

            file.close()
            return undo_log

    except FileNotFoundError:
        print("E404:file not found")

def process_instruction(instruction):
    global end_flag
    if instruction[0] == "END":
        end_flag = 1
    elif instruction[0] == "START" and instruction[1] == "CKPT":
        if end_flag == 1:
            print_variables()
            sys.exit(1)
    elif instruction[0] == "COMMIT":
        completed_transactions.append(instruction[1])
    elif instruction[0] == "START":
        pass
    else:
        transaction_id = instruction[0]
        var = instruction[1]
        value = int(instruction[2])
        if transaction_id not in completed_transactions:
            disk_variables[var] = value
        
def main(inp_file_path):
    
    end_flag = 0
    undo_log = read_and_parse_data(inp_file_path) 
    # print("ul",undo_log)

    for instruction in undo_log:
        process_instruction(instruction)
    
    print_variables()

if __name__ == "__main__":

    command=''
    if len(sys.argv) < 2:
        print("Please Enter a Query")
        print("Usage: python3 20171083_2.py 'Input_file_name'")
        sys.exit(1)
    else:
        command=sys.argv
        inp_file_path = command[1]
        main(inp_file_path)
    
    
