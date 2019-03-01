#!/usr/bin/python
# -*- coding:utf-8 -*-

from sys import argv  
import os
import sys
import re
import operator
import time

script, file_name, modifytype, columns, timepos,lookfor = argv

def find_text(file_name,lookfor):
    """
    mysqlbinlog处理后的binlog文件，根据查找关键字 look_key 
    查找具体的SQL语句，从SET TIMESTAMP 到 COMMIT 的一个区段完整信息
    传入参数：文件名,查找关键字
    """

    fin = open(file_name)
    fout = open(file_name + '.out','w+')
    need_write = False
    fout_list = ""

    for line in fin:
        if line[:13] == "SET TIMESTAMP":
            fout_list = ""
            need_write = False

            fout_list += line
        
        elif line[:11] == "COMMIT/*!*/":
            fout_list += line
            if need_write:
                fout.write(fout_list)
    
        else:        
            fout_list += line
            line_pos = line.find(lookfor)
            if line_pos > 0:
                need_write = True

    fin.close()
    fout.close()
    output_filename = file_name + ".out"
    return output_filename

def find_update(file_name, columns, timepos):
    """
    mysqlbinlog处理后的binlog文件，查找where 和 set 区段中列的具体值 
    通过传入表结构中列位置处理
    传入参数：文件名，列的位置, timestamp 字段列的位置
    """

    """输出文件名：原文件名后加 rollback"""
    fout = open(file_name + 'rollback','w+')
    fin = open(file_name)


    """第一次匹配where时, 还没有取得列的值,不需写到文件中去"""
    need_write = False
    content = ''
    sep = ','

    """列位置传进来的参数是字符串，转为list, 通过for循环处理 """
    posvalue = columns.split(',')
    timelist = timepos.split(',')

    """循环处理文件中的每一行"""
    for line in fin:
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
                colvalue =  re.findall(r"@" + onepos + "=(.*?)/\*",line)
                strcolvalue = sep.join(colvalue).rstrip()
                
                """
                binlog文件对 timestamp 列是以时间戳记录整型数字，需转换为字符串
                时间戳:1482325511 转换为：2016-12-21 21:05:11
                """
                if onepos in timelist and strcolvalue <> "0":
                    timeArray = time.localtime(int(strcolvalue))        
                    strcolvalue = "'" + time.strftime("%Y-%m-%d %H:%M:%S",timeArray) + "'"
                
                if onepos in timelist and strcolvalue == "0":
                    strcolvalue = "'0000-00-00 00:00:00'"
                    
                if len(content) == 0 :
                    content = strcolvalue
                else:
                    content += ',' + strcolvalue    
                                    
                need_write = True
            
    """
    处理完最后一个where区间时,已经跳出循环,需要补回一个 write 函数
    """
    fout.write(content + '\n')
    fin.close()
    fout.close()
    return
    
def find_insert(file_name):
    """
    mysqlbinlog处理后的binlog文件，查找 set 区段中列的具体值 
    提取第1列,主键的具体值
    传入参数：file_name
    """
    
    """输出文件名：原文件名后加 _rollback"""
    fout = open(file_name + '_rollback','w+')
    fin = open(file_name)


    """第一次匹配SET时, 还没有取得列的值,不需写到文件中去"""
    need_write = False
    content = ''
    
    """循环处理文件中的每一行"""
    for line in fin:

        """
        mysqlbinlog 解释出来的文件,对于insert语句,
        只写入更新后的值，因为insert之前是空
        以SET一个处理区间,只记录主键的值，按主键删除可以回滚    
        """
        if line[4:7] == "SET":
            if need_write:
                fout.write(content + '\n')
                content = ''
                need_write = False
                    
        elif operator.eq(re.findall(r"###   @(\d+)",line),['1']) == True: 
            colvalue =  re.findall(r"@1=(.*?)/\*",line)
            sep = ','
            
            if len(content) == 0 :
                content = sep.join(colvalue).rstrip()
            else:
                content += ',' + sep.join(colvalue).rstrip()    
                
            need_write = True
            
    """
    处理完最后一个set区间时,已经跳出循环,需要补回一个 write 函数
    """
    fout.write(content + '\n')
    fin.close()
    fout.close()
    return
    
def find_delete(file_name, timepos):
    """
    mysqlbinlog处理后的binlog文件，查找 where 区段中列的具体值 
    传入参数：file_name, timepos
    """
    
    """输出文件名：原文件名后加 _rollback"""
    fout = open(file_name + '_rollback','w+')
    fin = open(file_name)


    """第一次匹配where时, 还没有取得列的值,不需写到文件中去"""
    need_write = False
    content = ''
    sep = ','
    
    """时间字段列位置传进来的参数是字符串，转为list处理"""
    posvalue = timepos.split(',')

    """循环处理文件中的每一行"""
    for line in fin:

        """
        mysqlbinlog 解释出来的文件,对于delete语句,
        只写入删除前的值，因为delete之后是空
        以where为一个处理区间,记录所有列的值    
        """
        if line[4:9] == "WHERE":
            if need_write:
                fout.write(content + '\n')
                content = ''
                need_write = False
                    
        else:
            pos = sep.join(re.findall(r"###   @(\d+)",line))

            if len(pos) > 0:
                colvalue =  re.findall(r"@" + pos + "=(.*?)/\*",line) 
                strcolvalue = sep.join(colvalue).rstrip()

                """
                binlog文件对 timestamp 列是以时间戳记录整型数字，需转换为字符串
                时间戳:1482325511 转换为：2016-12-21 21:05:11
                """                
                if pos in posvalue and strcolvalue <> "0":
                    timeArray = time.localtime(int(strcolvalue))
                    strcolvalue = "'" + time.strftime("%Y-%m-%d %H:%M:%S",timeArray) + "'"
                
                if pos in posvalue and strcolvalue == "0":
                    strcolvalue = "'0000-00-00 00:00:00'"
                     
                if len(content) == 0 :
                    content = strcolvalue
                else:
                    content += ',' + strcolvalue
                                
                need_write = True
            
    """
    处理完最后一个set区间时,已经跳出循环,需要补回一个 write 函数
    """
    fout.write(content + '\n')

    fin.close()
    fout.close()
    return

"""对脚本传入参数进行判断"""
plist = ['DELETE','INSERT','UPDATE']

if modifytype.upper() not in plist:
    print "input modifytype is: ", modifytype," not right!!"
    print "scripts parameter modifytype is only: insert / delete / update "
    sys.exit(0)

if len(lookfor) == 0:
    print "the look for key must input something text"
    sys.exit(0)
    
if modifytype.upper() == "UPDATE" and (len(columns) == 0 or len(timepos) ==0):
    print "rollback UPDATE statement,must input the columns pos number"
    print "rollback UPDATE statement,timestamp columns must input pos number"
    sys.exit(0)

if modifytype.upper() == "UPDATE" and len(timepos) == 0:
    print "rollback UPDATE statement,must input the timestamp columns pos number"
    sys.exit(0)
    
""" 先根据关键字查找要回滚的SQL操作 """
process_filename = find_text(file_name,lookfor)

"""根据脚本传入的 modifytype 执行insert / delete / update 回滚SQL """

if modifytype.upper() == "INSERT":
    find_insert(process_filename)
    
elif modifytype.upper() == "DELETE":
    find_delete(process_filename, timepos)       

elif modifytype.upper() == "UPDATE":
    find_update(process_filename,columns,timepos)
