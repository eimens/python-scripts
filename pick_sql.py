#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
 mysqlbinlog处理后的binlog文件，根据查找关键字 lookfor 
 查找具体的SQL语句，从SET TIMESTAMP 到 COMMIT 的一个区段完整信息。
"""
from sys import argv  
import os

script, file_name, lookfor = argv

fin = open(file_name)
fout = open(file_name + '.out','a+')
need_write = False
fout_list = []

for line in fin.readlines():
    if line[:13] == "SET TIMESTAMP":
        fout_list = []
        need_write = False

        fout_list.append(line)
        
    elif line[:11] == "COMMIT/*!*/":
        fout_list.append(line)
        if need_write:
            for out_line in fout_list:
                fout.write(out_line)
    
    else:        
        fout_list.append(line)
        line_pos = line.find(lookfor)
        if line_pos > 0:
            need_write = True

fin.close()
fout.close()
