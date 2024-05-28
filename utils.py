import re
import os
from datetime import datetime
import time

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os
from datetime import datetime

class Disassembled_DDL():
    def __init__(self, DDL):
        self.table_type = self.type_table(DDL)
        self.table_db_name = self.table_name(DDL)  
        if 'create view' in DDL.lower():
            self.table_columns_s = None
            self.table_columns_p = None
            self.table_columns_c = None
            self.table_columns_skewed = None
            self.table_columns_skewed_on = None
            self.table_columns_skewed_stored= None
            self.as_sql = None
            self.other_parametrs = None
            self.like_table = None
            self.view = DDL.lower()
        elif 'like ' in DDL[DDL.find(self.table_name(DDL)) + len(self.table_db_name) - 1:].lower():
            self.table_columns_s = None
            self.table_columns_p = None
            self.table_columns_c = None
            self.table_columns_skewed = None
            self.table_columns_skewed_on = None
            self.table_columns_skewed_stored= None
            self.as_sql = None
            self.other_parametrs = self.everything_else(DDL)
            self.like_table = self.find_like(DDL)
            self.view = None
        else:
            self.table_columns_s = self.table_columns(DDL)
            self.table_columns_p = self.table_partitioned_columns(DDL)
            self.table_columns_c = self.table_clustered(DDL)
            self.table_columns_skewed = self.table_skewed(DDL)
            self.table_columns_skewed_on = self.table_on(DDL)
            self.table_columns_skewed_stored= self.stored_as_dir(DDL)
            self.as_sql = self.as_sql_statemant(DDL)
            self.other_parametrs = self.everything_else(DDL)
            self.like_table = None
            self.view = None
        
    
    
    def find_like(self, DDL):
        DDL = DDL.lower()
        
        pattern = r'like\s+\w+.\w+'

        substring = re.search(pattern, DDL)[0]
    
        substring = re.split(r'\s+', substring)[1]
        
        return substring
    
    def __str__(self):
        return f'''
        --------------table_type--------------
        {self.table_type}
        --------------table_db_name--------------
        {self.table_db_name}
        --------------table_columns_s--------------
        {self.table_columns_s}
        --------------table_columns_p--------------
        {self.table_columns_p}
        --------------table_columns_c--------------
        {self.table_columns_c}
        --------------table_columns_skewed--------------
        {self.table_columns_skewed}
        --------------table_columns_skewed_on--------------
        {self.table_columns_skewed_on}
        --------------table_columns_skewed_stored--------------
        {self.table_columns_skewed_stored}
        --------------as_sql--------------
        {self.as_sql}
        --------------other_parametrs--------------
        {self.other_parametrs}
        --------------like_table--------------
        {self.like_table}
        --------------like_table--------------
        {self.view}
        '''
    def table_name(self, DDL):
        DDL = DDL.lower()
    
        DDL = DDL.replace('if','').replace('not','').replace('exists','')
        
        pattern = r'table\s+\w+.\w+'
    
        substring = re.search(pattern, DDL)[0]
    
        substring = re.split(r'\s+', substring)[1]
        
        return substring
    
    def everything_else(self, DDL):
        DDL = DDL.lower()
    
        st = self.as_sql_statemant(DDL)
    
        if st:
            DDL = DDL.replace(st,'')
    
        r1 = DDL.find('row format')
        if 'stored as directories' in DDL:
            r2 = DDL.find('stored as', DDL.find('stored as directories')+len('stored as directories'))
        else:
            r2 = DDL.find('stored as')
        r3 = DDL.find('location')
        r4 = DDL.find('tblproperties')
        if r1 != -1:
            return DDL[r1:]
        elif r2 != -1:
            return DDL[r2:]
        elif r3 != -1:
            return DDL[r3:]
        elif r4 != -1:
            return DDL[r4:]
        else:
            return None

    def as_sql_statemant(self, DDL):
        DDL = DDL.lower()
        DDL = DDL.replace('stored as', '')
        if DDL.find('as') != -1:
            return DDL[DDL.find('as'):]
        else:
            return None

    def type_table(self, DDL):
        
        pattern = r'create \w+ table'
    
        try:
            substring = re.search(pattern, DDL.lower())[0]
        except:
            return None
    
        substring = substring.split(' ')
        
        return substring[1]
    
    
    def del_comma(self, DDL):
        l = 0
        r = 0
        DDL = list(DDL)
        for i in range(len(DDL)):
            if DDL[i] == '(':
                l += 1
            if DDL[i] == ')':
                r += 1
            if l > r and DDL[i] == ',':
                DDL[i] = '|'
            if l > r and DDL[i] == ' ':
                DDL[i] = ''
        return "".join(DDL)
    
    def del_spaces_between(self, DDL):
        l = 0
        DDL = list(DDL)
        for i in range(len(DDL)):
            if DDL[i] == "'":
                l += 1
            if l%2 == 1 and DDL[i] == ' ':
                DDL[i] = '_'
        return "".join(DDL)
    
    def del_spaces_from_list(self, test_list):
        res = []
        for ele in test_list:
            if ele.strip():
                res.append(ele)
        return res
    
    def table_columns(self, DDL):
        DDL = DDL.lower()
        
        name = self.table_name(DDL)
        
        pattern = name + r'\s+[(]'
    
        substring = re.search(pattern, DDL)
    
        if substring:
            first = DDL.find('(')
            last = first + 1
            l = 1
            r = 0
            while l != r:
                if DDL[last] == '(':
                    l += 1
                if DDL[last] == ')':
                    r += 1
                last += 1
                
            substring = DDL[first+1:last-1]
            substring = re.sub(r'\s+', ' ', substring)
    
            substring = self.del_comma(substring)
            substring = self.del_spaces_between(substring)
    
            substring = substring.split(',')
     
            
            cols = []
            contraints = []
            for el in substring:
                if not('primary key' in el) and not('constraint' in el):
                    el = self.del_spaces_from_list(el.split(' '))
    
                    name_of_col = el[0]
                    type_of_col = el[1]
                    if 'comment' in el:
                        col_comment = el[-1]
                    else: 
                        col_comment = None
                    
                    cols.append({
                        'name_of_col':name_of_col,
                        'type_of_col':type_of_col,
        
                        'col_comment':col_comment   
                    })
                else:
                    contraints.append(el.strip())
                
            return {'columns': cols,'constraints': contraints}
                    
            
        else:
            return None

    def table_partitioned_columns(self, DDL):
        DDL = DDL.lower()
        
        name = 'partitioned by'
        
        pattern = name + r'\s+[(]'
    
        substring = re.search(pattern, DDL)
    
        if substring:
            first = DDL.find(substring[0]) + len(substring[0])
            last = first + 1
            l = 1
            r = 0
            while l != r:
                if DDL[last] == '(':
                    l += 1
                if DDL[last] == ')':
                    r += 1
                last += 1
                
            substring = DDL[first:last-1]
            substring = re.sub(r'\s+', ' ', substring)
    
            substring = self.del_comma(substring)
            substring = self.del_spaces_between(substring)
    
            substring = substring.split(',')
     
            
            cols = []
            for el in substring:
                if not('primary key' in el) and not('constraint' in el):
                    el = self.del_spaces_from_list(el.split(' '))
                    if len(el) > 1:
                        name_of_col = el[0]
                        type_of_col = el[1]
                        if 'comment' in el:
                            col_comment = el[-1]
                        else: 
                            col_comment = None

                        cols.append({
                            'name_of_col':name_of_col,
                            'type_of_col':type_of_col,

                            'col_comment':col_comment   
                        })
                    else:
                        cols.append({
                            'name_of_col':el[0],
                            'type_of_col':None,

                            'col_comment':None   
                        })
                
            return {'partition_columns': cols}
                    
            
        else:
            return None

    def table_clustered(self, DDL):
        DDL = DDL.lower()
        
        pattern = 'clustered by' + r'.+' + r'into \d+ buckets'
    
        substring = re.search(pattern, DDL, flags=re.DOTALL)
        if substring:
            return substring[0]
        else:
            return None

    def table_skewed(self, DDL):
        DDL = DDL.lower()
        
        name = 'skewed by'
        
        pattern = name + r'\s+[(]'
    
        substring = re.search(pattern, DDL)
    
        if substring:
            first = DDL.find(substring[0]) + len(substring[0])
            last = first + 1
            l = 1
            r = 0
            while l != r:
                if DDL[last] == '(':
                    l += 1
                if DDL[last] == ')':
                    r += 1
                last += 1
                
            substring = DDL[first - len(substring[0]):last]
            return substring
        else:
            return None

    def table_on(self, DDL):
        DDL = DDL.lower()
        
        name = self.table_skewed(DDL)
        if name:
            DDL = DDL[DDL.find(name)+len(name):]

            pattern = 'on' + r'\s+[(]'

            substring = re.search(pattern, DDL, flags=re.DOTALL)

            if substring:
                first = DDL.find(substring[0]) + len(substring[0]) - 1
                last = first + 1
                l = 1
                r = 0
                while l != r:
                    if DDL[last] == '(':
                        l += 1
                    if DDL[last] == ')':
                        r += 1
                    last += 1

                substring = DDL[first - len(substring[0]):last]
                return substring.strip()
            else:
                return None
        else: 
            return None

    def stored_as_dir(self, DDL):
        DDL = DDL.lower()
        if 'stored as directories' in DDL:
            return 'stored as directories'
        else:
            return None

    def as_sql_statemant(self, DDL):
        DDL = DDL.lower()
        DDL = DDL.replace('stored as', '')
        if DDL.find('as') != -1:
#             return DDL[DDL.find('as'):]
            return None
        else:
            return None

    def everything_else(self, DDL):
        DDL = DDL.lower()
    
        st = self.as_sql_statemant(DDL)
    
        if st:
            DDL = DDL.replace(st,'')
    
        r1 = DDL.find('row format')
        if 'stored as directories' in DDL:
            r2 = DDL.find('stored as', DDL.find('stored as directories')+len('stored as directories'))
        else:
            r2 = DDL.find('stored as')
        r3 = DDL.find('location')
        r4 = DDL.find('tblproperties')
        if r1 != -1:
            return DDL[r1:]
        elif r2 != -1:
            return DDL[r2:]
        elif r3 != -1:
            return DDL[r3:]
        elif r4 != -1:
            return DDL[r4:]
        else:
            return None


def changed(p1, p2, type):
    if p1 != p2:
        a = '=====================================\n'
        a += f'{type}\n'
        a += '=====================================\n'
        a += f'{p1}\n'
        a += '-------------------------------------\n'
        a += 'changed_on\n'
        a += '-------------------------------------\n'
        a += f'{p2}\n'
        a += '=====================================\n'
        return a
    else:
        return False
        
def diff_DDLs(old_DDL, new_DDL):
    final_dict = {}
    a = ''
    # поля 
    if old_DDL.table_columns_s:
        old_cols = old_DDL.table_columns_s.get('columns')
    else:
        old_cols = {}
    
    if new_DDL.table_columns_s:
        new_cols = new_DDL.table_columns_s.get('columns')
    else:
        new_cols = {}
    
    s1 = set([el['name_of_col'] for el in old_cols])
    s2 = set([el['name_of_col'] for el in new_cols])

    del_columns = list(s1 - s2)
    new_columns = list(s2 - s1)
    other_columns = list((s2 - set(new_columns)))

    if del_columns:
        a += 'The following fields have been deleted from the table: ' + ', '.join(del_columns) + '\n'

    if new_columns:
        a += 'The following fields have been added to the table\n'
        for el in new_columns:
            for cols in new_cols:
                if cols['name_of_col'] == el:
                    if cols['col_comment']:
                        a += f"    {cols['name_of_col']} {cols['type_of_col'].replace('|', ',')} COMMENT {cols['col_comment']}\n"
                    else:
                        a += f"    {cols['name_of_col']} {cols['type_of_col'].replace('|', ',')}\n"
    if other_columns:
        
        for col in other_columns:
            for el1 in old_cols:
                for el2 in new_cols:
                    if (el1['name_of_col'] == col) and (col == el2['name_of_col']):
                        if (el1['type_of_col'] != el2['type_of_col']) or (el1['col_comment'] != el2['col_comment']):
                            a += 'The following fields have been changed in the table\n'
                            a += '====================================\n'
                            if el1['col_comment']:
                                a += f"   {el1['name_of_col']} {el1['type_of_col'].replace('|', ',')} COMMENT {el1['col_comment']}\n"
                            else:
                                a += f"   {el1['name_of_col']} {el1['type_of_col'].replace('|', ',')}\n"
                            a += '               ↓↓↓↓↓↓↓↓↓\n'
                            if el2['col_comment']:
                                a += f"   {el2['name_of_col']} {el2['type_of_col'].replace('|', ',')} COMMENT {el2['col_comment']}\n"
                            else:
                                a += f"   {el2['name_of_col']} {el2['type_of_col'].replace('|', ',')}\n"

    if a:
        final_dict['cols'] = [True, a]
    else:
        final_dict['cols'] = False
    
    # партиции
    a = ''
    if old_DDL.table_columns_p:
        old_cols = old_DDL.table_columns_p.get('partition_columns')
    else:
        old_cols = {}
    
    if new_DDL.table_columns_p:
        new_cols = new_DDL.table_columns_p.get('partition_columns')
    else:
        new_cols = {}
    
    s1 = set([el['name_of_col'] for el in old_cols])
    s2 = set([el['name_of_col'] for el in new_cols])

    del_columns = list(s1 - s2)
    new_columns = list(s2 - s1)
    other_columns = list((s2 - set(new_columns)))

    if del_columns:
        a += 'The following partitions fields have been deleted from the table: ' + ', '.join(del_columns) + '\n'

    if new_columns:
        a += 'The following partitions fields have been added to the table\n'
        for el in new_columns:
            for cols in new_cols:
                if cols['name_of_col'] == el:
                    if cols['col_comment']:
                        a += f"   {cols['name_of_col']} {cols['type_of_col'].replace('|', ',')} COMMENT {cols['col_comment']}\n"
                    else:
                        a += f"   {cols['name_of_col']} {cols['type_of_col'].replace('|', ',')}\n"
    if other_columns:
        for col in other_columns:
            for el1 in old_cols:
                for el2 in new_cols:
                    if (el1['name_of_col'] == col) and (col == el2['name_of_col']):
                        if (el1['type_of_col'] != el2['type_of_col']) or (el1['col_comment'] != el2['col_comment']):
                            a += 'The following partitions fields have been changed in the table\n'
                            a += '====================================\n'
                            if el1['col_comment']:
                                a += f"   {el1['name_of_col']} {el1['type_of_col'].replace('|', ',')} COMMENT {el1['col_comment']}\n"
                            else:
                                a += f"   {el1['name_of_col']} {el1['type_of_col'].replace('|', ',')}\n"
                            a += '               ↓↓↓↓↓↓↓↓↓\n'
                            if el2['col_comment']:
                                a += f"   {el2['name_of_col']} {el2['type_of_col'].replace('|', ',')} COMMENT {el2['col_comment']}\n"
                            else:
                                a += f"   {el2['name_of_col']} {el2['type_of_col'].replace('|', ',')}\n"

    if a:
        final_dict['part_cols'] = [True, a]
    else:
        final_dict['part_cols'] = False
    # Всё остальное

    
    
    
    
    table_type = changed(old_DDL.table_type, new_DDL.table_type, 'table_type')
    
    if table_type:
        final_dict['table_type'] = [True, table_type]
    else:
        final_dict['table_type'] = False

    table_db_name = changed(old_DDL.table_db_name, new_DDL.table_db_name, 'table_db_name')
    
    if table_db_name:
        final_dict['table_db_name'] = [True, table_db_name]
    else:
        final_dict['table_db_name'] = False

    table_columns_c = changed(old_DDL.table_columns_c, new_DDL.table_columns_c, 'table_columns_c')
    
    if table_columns_c:
        final_dict['table_columns_c'] = [True, table_columns_c]
    else:
        final_dict['table_columns_c'] = False

    table_columns_skewed = changed(old_DDL.table_columns_skewed, new_DDL.table_columns_skewed, 'table_columns_skewed')
    
    if table_columns_skewed:
        final_dict['table_columns_skewed'] = [True, table_columns_skewed]
    else:
        final_dict['table_columns_skewed'] = False

    table_columns_skewed_on = changed(old_DDL.table_columns_skewed_on, new_DDL.table_columns_skewed_on, 'table_columns_skewed_on')
    
    if table_columns_skewed_on:
        final_dict['table_columns_skewed_on'] = [True, table_columns_skewed_on]
    else:
        final_dict['table_columns_skewed_on'] = False

    table_columns_skewed_stored = changed(old_DDL.table_columns_skewed_stored, new_DDL.table_columns_skewed_stored, 'table_columns_skewed_stored')
    
    if table_columns_skewed_stored:
        final_dict['table_columns_skewed_stored'] = [True, table_columns_skewed_stored]
    else:
        final_dict['table_columns_skewed_stored'] = False

    as_sql = changed(old_DDL.as_sql, new_DDL.as_sql, 'as_sql')
    
    if as_sql:
        final_dict['as_sql'] = [True, as_sql]
    else:
        final_dict['as_sql'] = False

    other_parametrs = changed(old_DDL.other_parametrs, new_DDL.other_parametrs, 'other_parametrs')
    
    if other_parametrs:
        final_dict['other_parametrs'] = [True, other_parametrs]
    else:
        final_dict['other_parametrs'] = False

    like_table = changed(old_DDL.like_table, new_DDL.like_table, 'like_table')
    
    if like_table:
        final_dict['like_table'] = [True, like_table]
    else:
        final_dict['like_table'] = False

    return final_dict

def send_message(emails, message):
    for email in emails:
        msg = MIMEMultipart()
        message = message
        password = ''
        msg['From'] = "ann-kyprianova2002@yandex.ru"
        msg['To'] = email
        msg['Subject'] = "Subscription"
        msg.attach(MIMEText(message, 'plain'))
        server = smtplib.SMTP('smtp.yandex.ru', 587)
        server.starttls()
        server.login(msg['From'], password)
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        server.quit()
        
def do_message(res):
    a = ''
    for key, value in res.items():
        if value:
            a += value[1]
    return a

def inspect_table(table_name, DDL):
    db, name = table_name.split('.')
    main_path = '/notebook/DDL_databases'
    if os.path.exists(f'{main_path}/{db}/{name}'):
        files = os.listdir(f'{main_path}/{db}/{name}')
        for el in files:
            if 'hql' in el:
                name_of_hql_file = el
        
        with open(f'{main_path}/{db}/{name}/{name_of_hql_file}') as f:
            s = f.read()
        
        new = Disassembled_DDL(DDL)
        old = Disassembled_DDL(s)

        res = diff_DDLs(old, new)
        
        k = 0
        for key, value in res.items():
            if value:
                k = 1
        
        if k == 0:
            os.remove(f'{main_path}/{db}/{name}/{name_of_hql_file}')
            now = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
            my_file = open(f'{main_path}/{db}/{name}/Last_{now}.hql', "w+")
            my_file.write(DDL)
            my_file.close()
        else:
            with open(f'{main_path}/{db}/{name}/{name_of_hql_file}') as f:
                old_file = f.read()

            my_file = open(f'{main_path}/{db}/{name}/History/Last_{name_of_hql_file}.hql', "w+")
            my_file.write(old_file)
            my_file.close()
            
            os.remove(f'{main_path}/{db}/{name}/{name_of_hql_file}')
            now = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
            my_file = open(f'{main_path}/{db}/{name}/Last_{now}.hql', "w+")
            my_file.write(DDL)
            my_file.close()

            send_message(['vovalagutov1111@gmail.com'], do_message(res))
    else:
        os.mkdir(f'{main_path}/{db}/{name}')
        os.mkdir(f'{main_path}/{db}/{name}/History')
        now = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
        my_file = open(f'{main_path}/{db}/{name}/Last_{now}.hql', "w+")
        my_file.write(DDL)
        my_file.close()

        my_file = open(f'{main_path}/{db}/{name}/Sender_file.txt', "w+")
        my_file.write('vovalagutov1111@gmail.com')
        my_file.close()

        send_message(['vovalagutov1111@gmail.com'], f'add new table {db}.{name}')
        
def recursive_file_gen(mydir):
    for root, dirs, files in os.walk(mydir):
        for file in files:
            yield os.path.join(root, file)

def all_tables():
    main_path = '/notebook/DDL_databases'
    all_files = list(recursive_file_gen(f'{main_path}/'))
    all_t = []
    for el in all_files:
        if not('History' in el) and ('hql' in el):
            db, name = el[24:-29].split('/')
            all_t.append(f'{db}.{name}')
    return all_t

def all_tables_in_hadoop(spark):
    
    all_databases = spark.sql("SHOW DATABASES").toPandas().iloc[:, 0].tolist()
    all_tables = []
    for db in all_databases:
        tbls = spark.sql(f"SHOW TABLES IN {db}").toPandas().iloc[:, 1].tolist()
        for tbl in tbls:
            all_tables.append(f'{db}.{tbl}')
    return all_tables

def deleted_tables(spark):
    all_tables_in_hd = set(all_tables_in_hadoop(spark))
    all_saved_tbls = set(all_tables())

    del_t = all_saved_tbls - all_tables_in_hd
    
    if del_t:
        body = 'This tables was deleted from hadoop:'
        body += '\n'.join(list(del_t))

        send_message(['vovalagutov1111@gmail.com'], body)
        
def main_alerting_function(spark):
    
    all_databases = spark.sql("SHOW DATABASES").toPandas().iloc[:, 0].tolist()
    all_databases.remove('default')
    all_tables = []
    for db in all_databases:
        tbls = spark.sql(f"SHOW TABLES IN {db}").toPandas().iloc[:, 1].tolist()
        for tbl in tbls:
            t = f'{db}.{tbl}'
            inspect_table(t, spark.sql(f'SHOW CREATE TABLE {t}').toPandas().iloc[0,0])

    deleted_tables(spark)
    
def alter_table(spark, table_name, new_DDL, select_statement):    
    
    db, name = table_name.split('.')
    main_path = '/notebook/DDL_databases'
    files = os.listdir(f'{main_path}/{db}/{name}')
    
    for el in files:
        if 'hql' in el:
            name_of_hql_file = el

    print('===============================================')
    print('start testing DDLs')
    try:
        with open(f'{main_path}/{db}/{name}/{name_of_hql_file}') as f:
            old_file = f.read()
        D_DDL = Disassembled_DDL(old_file)
        old_file = old_file.replace(D_DDL.table_db_name, D_DDL.table_db_name + '__tmp__delete_it_pls')
        D_DDL = Disassembled_DDL(old_file)
        spark.sql(old_file).toPandas()
        print('Success create table from file')
        spark.sql(f'DROP TABLE IF EXISTS {D_DDL.table_db_name}').toPandas()
        print('Success drop table from file')
    except:
        print('Failed. Misstake in saved file')
        return False

    try:
        D_DDL = Disassembled_DDL(new_DDL)
        newest_DDL = new_DDL.replace(D_DDL.table_db_name, D_DDL.table_db_name + '__tmp__delete_it_pls')
        D_DDL = Disassembled_DDL(newest_DDL)
        spark.sql(newest_DDL).toPandas()
        print('Success create table new_DDL')
        spark.sql(f'DROP TABLE IF EXISTS {D_DDL.table_db_name}').toPandas()
        print('Success drop table new_DDL')
    except:
        print('Failed. Misstake in new_DDL')
        return False

    print('===============================================')
    
    print('start proccess')

    print('start create backup table')

    with open(f'{main_path}/{db}/{name}/{name_of_hql_file}') as f:
        old_file = f.read()

    D_DDL = Disassembled_DDL(old_file)
    old_file = old_file.replace(D_DDL.table_db_name, D_DDL.table_db_name + '__backup')

    spark.sql(old_file).toPandas()

    print('start upload data to backup table')

    spark.sql(f'SELECT * FROM {D_DDL.table_db_name}').write.insertInto(D_DDL.table_db_name + '__backup')
    

    cnt_old = spark.sql(f"SELECT COUNT(*) as cnt FROM {D_DDL.table_db_name}").toPandas().iloc[0, 0]
    cnt_backup =  spark.sql(f"SELECT COUNT(*) as cnt FROM {D_DDL.table_db_name + '__backup'}").toPandas().iloc[0, 0]

    if cnt_old != cnt_backup:
        raise Exception('cnt_old != cnt_backup')
    print('Succes cnt_old == cnt_backup')
    print('Finish upload data to backup table')
    print('===============================================')
    print('Start drop and recreate main table')
    
    with open(f'{main_path}/{db}/{name}/{name_of_hql_file}') as f:
        old_file = f.read()
    D_DDL = Disassembled_DDL(old_file)

    spark.sql(f'DROP TABLE IF EXISTS {D_DDL.table_db_name}').toPandas()
    spark.sql(new_DDL).toPandas()
    print('Success recreate')
    print('start upload data to main table')

    D_DDL = Disassembled_DDL(new_DDL)
    spark.sql(select_statement).coalesce(1).write.format('ORC').insertInto(D_DDL.table_db_name)
    spark.sql(f"DROP TABLE {D_DDL.table_db_name}__backup")

    print('Success. Finish work')
