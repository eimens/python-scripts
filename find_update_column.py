#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
 mysqlbinlog处理后的binlog文件，查找where 和 set 区段中列的具体值 
 通过传入表结构中列位置处理
 pthon ./find_column_value.py file_name "1,3,5,6"  --提取第1,3,5,6列的具体值
"""

from sys import argv  
import os
import re
import operator

script, file_name, columnpos = argv

"""输出文件名：原文件名后加 .out"""
fout = open(file_name + '.out','w+')
fin = open(file_name)


"""第一次匹配where时, 还没有取得列的值,不需写到文件中去"""
need_write = False
content = ''

"""列位置传进来的参数是字符串，转为list, 通过for循环处理 """
posvalue = columnpos.split(',')

"""循环处理文件中的每一行"""
for line in fin.readlines():

    """对每一行文件,每个列pos都匹配一次,stupid logic"""
    for onepos in  posvalue:   
        poslist = onepos.split()
        
        """
        mysqlbinlog 解释出来的文件,以where为一个处理区间
        传入参数1,3，取第1和第3列的数据，会得到4个数值
        分别是更新前和更新后的值
        """
        if line[4:9] == "WHERE":
            if need_write:
                fout.write(content + '\n')
                content = ''
                need_write = False
                    
        elif operator.eq(re.findall(r"###   @(\d+)",line),poslist) == True: 
            colvalue =  re.findall(r"@" + onepos + "=([^/*]+)",line)
            sep = ','
            
            if len(content) == 0 :
                content = sep.join(colvalue).rstrip()
            else:
                content += ',' + sep.join(colvalue).rstrip()    
                
            need_write = True
            
"""
处理完最后一个where区间时,已经跳出循环,需要补回一个 write 函数
"""
fout.write(content + '\n')

fin.close()
fout.close()
