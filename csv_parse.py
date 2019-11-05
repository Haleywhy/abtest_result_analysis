#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv
from collections import defaultdict

class csv_parser:
    
    def __init__(self, filename, delimiter, desired_cols, batchsize):
        self.file = filename
        self.column = desired_cols
        self.batchsize = batchsize
        self.delimiter = delimiter
    
    def validate_desired_cols(self, header):
        for col in self.column:
            if col in header:
                pass
            else:
                raise ValueError("'{column}' is not valid.", column = col)
    
    def read_data(self, reader):
        table = defaultdict(list)
        count = 0
        for row in reader:
            for (k,v) in enumerate(row):
                if k in self.column:
                    table[k].append(v)
            count += 1
            if count == self.batchsize:
                load_to_db(table)
                table = defaultdict(list)
                count = 0  
        
    
    def parse(self):    
        with open(self.file, 'r') as csv_file:
            header = csv_file.readline().rstrip().split(',')
            validate_desired_cols(header) 
            reader = csv.DictReader(csv_file, delimiter = self.delimiter)
            read_data(reader)    
        csv_file.close()
          
    
    def load_to_db(self, table):
        # handle repeated rows
        pass
        

