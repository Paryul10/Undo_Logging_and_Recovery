import os
import sys
import re
import copy
import time
from itertools import groupby

DEBUG_FLAG = 0
disk_variables = {}
memory_variables = {}
operations = ['+','-','*','/']
all_transactions = []
transaction_ids = []
transaction_lengths = []
temp_mem_vars = {}
X = 0
undo_log = []
all_transactions_len = 0

def print_variables():
    j = 0
    lens = len(memory_variables.keys())
    for i in sorted (memory_variables.keys()): 
        if j == lens-1:
            print(i,memory_variables[i], end = "")
        else:
            print(i,memory_variables[i], end = " ")
        j += 1
    print()

    j = 0
    lens = len(disk_variables.keys())
    for i in sorted (disk_variables.keys()): 
        if j == lens - 1:
            print(i,disk_variables[i], end = "")
        else:
            print(i,disk_variables[i], end = " ")
        j += 1
    print()

def seperate_transactions(all_transactions):
    all_transactions = [list(group) for k, group in groupby(all_transactions, lambda x: x == "") if not k]
    total_transactions = len(all_transactions)
    if DEBUG_FLAG == 1:
        print(all_transactions)
    
    for i in range(total_transactions):
        transaction_ids.append(all_transactions[i][0][0])
        transaction_lengths.append(len(all_transactions[i]))
    
    for i in range(total_transactions):
        instructions = len(all_transactions[i])
        for j in range(instructions):
            if j == 0:
                pass
            else:
                all_transactions[i][j].append(transaction_ids[i])
    
    return all_transactions

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
                        if lines[i].find(':=') != -1:
                            ## transaction
                            lines[i] = lines[i].split(" := ")
                            temp.append(lines[i][0])
                            for operation in operations:
                                if operation in lines[i][1]:
                                    tt = lines[i][1].split(operation)
                                    temp.append(tt[0])
                                    temp.append(operation)
                                    temp.append(tt[1])
                            
                        elif (lines[i].find("READ") != -1):
                            ## READ
                            temp.append("READ")
                            lines[i] = lines[i].replace('READ', '').strip().strip('()').split(',')
                            for varss in lines[i]:
                                temp.append(varss)

                        elif (lines[i].find('WRITE') != -1):
                            ## WRITE
                            temp.append("WRITE")
                            lines[i] = lines[i].replace('WRITE', '').strip().strip('()').split(',')
                            for varss in lines[i]:
                                temp.append(varss)

                        elif (lines[i].find('OUTPUT') != -1):
                            ## OUTPUT
                            temp.append("OUTPUT")
                            lines[i] = lines[i].replace('OUTPUT', '').strip().strip('()').split(',')
                            for varss in lines[i]:
                                temp.append(varss)
                        else:
                            ## Transaction name and num of instructions == T1 8
                            lines[i] = lines[i].split(' ')
                            for varss in lines[i]:
                                temp.append(varss)
                        
                        all_transactions.append(temp)
                else:
                    all_transactions.append('') 
                        
                i += 1

            if DEBUG_FLAG == 1:
                print(disk_variables)
            
            if DEBUG_FLAG == 1:
                for instruction in all_transactions:
                    print(instruction)

            file.close()

            return all_transactions

    except FileNotFoundError:
        print("E404:file not found")

def process_instruction(instruction,is_last):
    
    ins_len = len(instruction)
    temp = []

    if ins_len == 2:
        ## Start the transaction and log it
        temp.append("START")
        temp.append(instruction[0])
        undo_log.append(temp)
        print("<START "+instruction[0] + ">")
        print_variables()
    else:
        if instruction[0] == "READ":
            ## read the memory / disk variable into the temp_mem_vars
            var = instruction[1]
            temp_var = instruction[2]
            
            if var in memory_variables.keys():
                ### if variable in memory
                temp_mem_vars[temp_var] = memory_variables[var]
            else:
                ### else create in memory 
                memory_variables[var] = disk_variables[var]
                temp_mem_vars[temp_var] = memory_variables[var]

        elif instruction[0] == "WRITE":
            ## write the value in memory and update the log file
            var = instruction[1]
            temp_var = instruction[2]
            transaction_id = instruction[3]
            previous_val = memory_variables[var]
            memory_variables[var] = temp_mem_vars[temp_var]

            ### log string
            temp.append(transaction_id)
            temp.append(var)
            temp.append(previous_val)

            undo_log.append(temp)
            print("<"+transaction_id+", "+var+", "+str(previous_val)+">")
            print_variables()

        elif instruction[0] == "OUTPUT":
            ## write into log file
            var = instruction[1]
            transaction_id = instruction[2]
            disk_variables[var] = memory_variables[var]

            temp.append("COMMIT")
            temp.append(transaction_id)

            undo_log.append(transaction_id)
            if is_last:
                print("<COMMIT "+transaction_id+">")
                print_variables()

        else:
            ## do the operation
            temp_var_w = instruction[0]
            temp_var_r = instruction[1]
            operator = instruction[2]
            val = int(instruction[3])

            if operator == operations[0]:
                temp_mem_vars[temp_var_w] = temp_mem_vars[temp_var_r] + val
            elif operator == operations[1]:
                temp_mem_vars[temp_var_w] = temp_mem_vars[temp_var_r] - val
            elif operator == operations[2]:
                temp_mem_vars[temp_var_w] = temp_mem_vars[temp_var_r] * val
            else:
                temp_mem_vars[temp_var_w] = temp_mem_vars[temp_var_r] / val

def main():

    all_transactions = read_and_parse_data(inp_file_path)
    all_transactions = seperate_transactions(all_transactions)

    if DEBUG_FLAG == 1:
        for instruction in all_transactions:
            print(instruction)
        print(memory_variables,disk_variables)
    
    left_ins_trans = []
    ins_trans = []
    all_transactions_len = 0
    for length in transaction_lengths:
        all_transactions_len += length
        left_ins_trans.append(length)
        ins_trans.append(0)

    if DEBUG_FLAG == 1:
        print(left_ins_trans,ins_trans)
    
    ## rr loop
    current_transaction = 0
    i = 0
    while i < all_transactions_len:
        j = 0
        while(j<int(X)):
            if left_ins_trans[current_transaction] > 0:
                # print("Instruction",ins_trans[current_transaction],"of transaction",current_transaction)
                # print(all_transactions[current_transaction][ins_trans[current_transaction]])
                if left_ins_trans[current_transaction] == 1:
                    process_instruction(all_transactions[current_transaction][ins_trans[current_transaction]],True)
                else:
                    process_instruction(all_transactions[current_transaction][ins_trans[current_transaction]],False)
                if ins_trans[current_transaction] == 0:
                    j -= 1
                ins_trans[current_transaction] += 1
                left_ins_trans[current_transaction] -= 1
                i += 1
                j += 1
            else:
                j += 1
                pass
        current_transaction = (current_transaction + 1) % len(transaction_lengths)
        # time.sleep(5)
        # print()
       
if __name__ == "__main__":

    command=''
    if len(sys.argv) < 2:
        print("Please Enter a Query")
        print("Usage: python3 20171083_1.py 'input_file_name' 'X'")
        sys.exit(1)
    else:
        command=sys.argv
        inp_file_path = command[1]
        X = command[2]

        if DEBUG_FLAG == 1:
            print(inp_file_path,X)
        
        main()