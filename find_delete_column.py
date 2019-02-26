#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
 mysqlbinlog处理后的binlog文件，查找 where 区段中列的具体值 
 pthon ./find_delete_column file_name    
"""

from sys import argv  
import re

script, file_name = argv

"""输出文件名：原文件名后加 _insert.out"""
fout = open(file_name + '_insert.out','w+')
fin = open(file_name)


"""第一次匹配where时, 还没有取得列的值,不需写到文件中去"""
need_write = False
content = ''
sep = ','

"""循环处理文件中的每一行"""
for line in fin.readlines():

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
            colvalue =  re.findall(r"@" + pos + "=([^/*]+)",line) 

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
