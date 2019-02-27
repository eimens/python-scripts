#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
 mysqlbinlog处理后的binlog文件，查找 set 区段中列的具体值 
 pthon ./find_insert_column.py file_name    --提取第1列,主键的具体值
"""

from sys import argv  
import re
import operator

script, file_name = argv

"""输出文件名：原文件名后加 _rollback.out"""
fout = open(file_name + '_rollback.out','w+')
fin = open(file_name)


"""第一次匹配SET时, 还没有取得列的值,不需写到文件中去"""
need_write = False
content = ''


"""循环处理文件中的每一行"""
for line in fin.readlines():

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
