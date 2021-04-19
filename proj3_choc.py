###    Name: Hang Song    ###
### unique name: hangsong ###

import sqlite3
import re
import plotly.graph_objects as go
from columnar import columnar

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from a database called choc.db
DBNAME = 'choc.sqlite'
conn = sqlite3.connect(DBNAME)
cur = conn.cursor



def process_bars(where, sell_source, order_by, top_bottom, num_results):
    ''' Construct the query string from passed variables process bars command type
    Parameters
    ----------
    where: a list of string of country/region and corresponding name
    sell_source: string, filtered by whether source or sell
    order_by: string, order by what criteria e.g. ratings,cocoa,number_of_bars
    top_bottom: string, display from the descending order (top) or ascending order (bottom)
    num_results: string, isnumeric, displace how many results

    Returns
    -------
    query_string: string
        a query string to excute SQL
    '''

    query_string = ''' '''
    # SELECT VARIABLES --------------------------------------------------------------------
    select_string ='''SELECT SpecificBeanBarName, Company, sell.EnglishName, Rating, CocoaPercent, source.EnglishName\nFROM Bars'''

    # JOIN Tables ------------------------------------------------------------------------
    join = '''\nJOIN Countries as sell ON Bars.CompanyLocationId = sell.Id'''
    join2 = '''\nJOIN Countries as source ON Bars.BroadBeanOriginId = source.Id'''

    # Specify condition whether it is sell/source ------------------------------------------------------------------------
    where_string = ''' '''
    if len(where) != 0:
        if sell_source == 'sell':
            where_string = f'''\nWHERE sell.{where[0]} = "{where[1]}" '''
        elif sell_source == 'source':
            where_string = f'''\nWHERE source.{where[0]} = "{where[1]}" '''

    # Sort by what criteria and how many results will be returned ----------------------------------------
    orderby_limit_string = f'''\nORDER BY {order_by} {top_bottom} LIMIT {num_results}'''
    query_string = select_string+join+join2+where_string+orderby_limit_string
    return (query_string)

def process_companies(where, order_by, top_bottom, num_results):
    ''' Construct the query string from passed variables to process company command type
    Parameters
    ----------
    where: a list of string of country/region and corresponding name
    order_by: string, order by what criteria e.g. ratings,cocoa,number_of_bars
    top_bottom: string, display from the descending order (top) or ascending order (bottom)
    num_results: string, isnumeric, displace how many results

    Returns
    -------
    query_string: string
        a query string to excute SQL
    '''

    query_string = ''' '''
    # SELECT VARIABLES (Company, companylocation)--------------------------------------------------------------------------------------------
    select_string =f'''SELECT Company, sell.EnglishName,'''
    # SELECT THE THIRD VARIABLE, CRITERIA (ratings,cocoa,num_of_bars)--------------------------------------------------------------------------------------------
    if order_by =='Rating' or order_by == 'CocoaPercent':
        select_string = select_string+f'''avg({order_by}) \nFROM Bars'''
    else:
        select_string = select_string+f'''{order_by} \nFROM Bars'''
    # JOIN Tables ------------------------------------------------------------------------
    join = '''\nJOIN Countries as sell ON Bars.CompanyLocationId = sell.Id'''

    # Specify country/region condition ------------------------------------------------------------
    where_string = ''' '''
    if len(where) != 0:
        where_string = f'''\nWHERE sell.{where[0]} = "{where[1]}" ''' #companies can only be sellers
    
    # Groupby and having statment with num_of_bars > 4 ---------------------------------------------------
    group_have_string = f'''GROUP BY Bars.Company \nHaving COUNT(*)>4'''
    
    # Sort by what criteria and how many results will be returned ----------------------------------------
    orderby_limit_string =''' '''
    if order_by =='Rating' or order_by == 'CocoaPercent':
        orderby_limit_string = f'''\nORDER BY avg({order_by}) {top_bottom} LIMIT {num_results}'''
    else:
        orderby_limit_string = f'''\nORDER BY {order_by} {top_bottom} LIMIT {num_results}'''
    query_string = select_string+join+where_string+group_have_string+orderby_limit_string
    return (query_string)

def process_countries(where, sell_source, order_by, top_bottom, num_results):
    ''' Construct the query string from passed variables to process countries command type
    Parameters
    ----------
    where: a list of string of region and corresponding name. (Country is invalid)
    sell_source: string, filtered by whether source or sell
    order_by: string, order by what criteria e.g. ratings,cocoa,number_of_bars
    top_bottom: string, display from the descending order (top) or ascending order (bottom)
    num_results: string, isnumeric, displace how many results

    Returns
    -------
    query_string: string
        a query string to excute SQL
    '''

    query_string = ''' '''

    # SELECT VARIABLES (Country_name, region_name)--------------------------------------------------------------------------------------------
    select_string =f'''SELECT C.EnglishName, C.Region,'''

    # SELECT THE THIRD VARIABLE, CRITERIA (ratings,cocoa,num_of_bars)--------------------------------------------------------------------------------------------
    if order_by =='Rating' or order_by == 'CocoaPercent':
        select_string = select_string+f'''avg({order_by}) \nFROM Bars'''
    else:
        select_string = select_string+f'''{order_by} \nFROM Bars'''

    # JOIN Tables ------------------------------------------------------------------------
    join = ''' '''
    if sell_source == "source":
        join = '''\nJOIN Countries as C ON Bars.BroadBeanOriginId = C.Id'''
    else:
        join = '''\nJOIN Countries as C ON Bars.CompanyLocationId = C.Id'''

    # Specify country/region condition ------------------------------------------------------------
    where_string = ''' '''
    if len(where) != 0:
        where_string = f'''\nWHERE C.{where[0]} = "{where[1]}" '''

    # Groupby and having statment with num_of_bars > 4 ---------------------------------------------------
    group_have_string = f'''\nGROUP BY C.EnglishName \nHaving COUNT(*)>4'''

    # Sort by what criteria and how many results will be returned ----------------------------------------
    orderby_limit_string = ''' '''
    if order_by =='Rating' or order_by == 'CocoaPercent':
        orderby_limit_string = f'''\nORDER BY avg({order_by}) {top_bottom} LIMIT {num_results}'''
    else:
        orderby_limit_string = f'''\nORDER BY {order_by} {top_bottom} LIMIT {num_results}'''
    query_string = select_string+join+where_string+group_have_string+orderby_limit_string

    return (query_string)

def process_regions(sell_source, order_by, top_bottom, num_results):
    ''' Construct the query string from passed variables to process regions command type
    Parameters
    ----------
    sell_source: string, filtered by whether source or sell
    order_by: string, order by what criteria e.g. ratings,cocoa,number_of_bars
    top_bottom: string, display from the descending order (top) or ascending order (bottom)
    num_results: string, isnumeric, displace how many results

    Returns
    -------
    query_string: string
        a query string to excute SQL
    '''

    query_string = ''' '''

    # SELECT VARIABLES (Country_name, region_name)--------------------------------------------------------------------------------------------
    select_string =f'''SELECT C.Region, '''
    # SELECT THE SECOND VARIABLE, CRITERIA (ratings,cocoa,num_of_bars)--------------------------------------------------------------------------------------------
    if order_by =='Rating' or order_by == 'CocoaPercent':
        select_string = select_string+f'''avg({order_by}) \nFROM Bars'''
    else:
        select_string = select_string+f'''{order_by} \nFROM Bars'''

    # JOIN Tables ------------------------------------------------------------------------
    join = ''' '''
    if sell_source == "source":
        join = '''\nJOIN Countries as C ON Bars.BroadBeanOriginId = C.Id'''
    else:
        join = '''\nJOIN Countries as C ON Bars.CompanyLocationId = C.Id'''

    # Groupby and having statment with num_of_bars > 4 ---------------------------------------------------
    group_have_string = f'''\nGROUP BY C.Region \nHaving COUNT(*)>4'''

    # Sort by what criteria and how many results will be returned ----------------------------------------
    orderby_limit_string =''' '''
    if order_by =='Rating' or order_by == 'CocoaPercent':
        orderby_limit_string = f'''\nORDER BY avg({order_by}) {top_bottom} LIMIT {num_results}'''
    else:
        orderby_limit_string = f'''\nORDER BY {order_by} {top_bottom} LIMIT {num_results}'''
    query_string = select_string+join+group_have_string+orderby_limit_string

    return (query_string)

def plot_bar(query_result,command_type,order_by,top_bottom,num_results):
    ''' plot the barplot based on the query result
    Parameters
    ----------
    query_result
        a list of tuples that represent the fetched query results
    command_type
        command type to determine the values in x axis
    order_by e.g. avg(cocoa),avg(rating),number_of_bars)
        sorting criteria to determine the values in y axis
    top_bottom: string, whether top or bottom
    num_results: integer, how many results will be returned
    Returns
    -------
    None

    '''
    xvals = []
    yvals = []

    ## Bars type-----------------------------------------------------
    ## Valid group_by is rating and cocoa
    if command_type == 'bars':
        for row in query_result:
            xvals.append(row[0])
            if order_by == 'Rating':
                yvals.append(row[3])
            else:
                yvals.append(row[4]) 

    ## Regions, Companies, Countries type----------------------------------------------------
    else:
        for row in query_result:
            xvals.append(row[0])
            yvals.append(row[-1])

    ## Formatting -----------------------------------------------------------------
    if top_bottom == 'DESC':
        top_bottom = 'Bottom'
    else:
        top_bottom = 'Top'
    
    bar_data = go.Bar(x=xvals, y=yvals)
    if order_by == "COUNT(*)":
        basic_layout = go.Layout(title=f"{top_bottom} {num_results} {command_type} sorted by number of bars")
    else:
        basic_layout = go.Layout(title=f"{top_bottom} {num_results} {command_type} sorted by {order_by}")
    fig = go.Figure(data=bar_data, layout=basic_layout)

    fig.show()


def show_pretty_table(raw_query_result):
    '''
    Display result in a nicelt patterned way
    Parameters
    --------------------
    Raw_query_result: list of queries matched found in SQL
    Return
    ---------------------
    String
    - Table: strings, nicely formatted
    - or "Command not recognized" from raw query result
    '''
    clean_data = []
    table = None
    if "Command not recognized" not in raw_query_result:
        for i in raw_query_result:
            singledata = []
            for j in i:
                if type(j) == str:
                    if len(j)>12:
                        j = j[:12]+'...'
                    else:
                        j = "{:<15}".format(j)
                elif type(j) == int:
                    j = "{:<15}".format(j)
                else:
                    j = "{:15.2f}".format(j)
                singledata.append(j)
            clean_data.append(singledata)
        datainput = clean_data
        table = columnar(data=datainput,headers=None, no_borders=False)
        return table
    else:
        return raw_query_result


# Part 1: Implement logic to process user commands
def process_command(command):
    command_list = command.split()
    parameters_list = command_list[1:]

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    ## Blank input invalid----------------------------------------------------------
    if len(command_list) == 0:
        return f'Command not recognized: {command}\n'

    ### Construct default variables -------------------------------------------------
    command_type = 'NA'
    where = []
    sell_source = 'NA'
    order_by = 'Rating' #default=ratings which is Rating
    top_bottom = 'DESC' #default is top
    num_results = 10
    barplot = 'No'

    ### Specify command type --------------------------------------------------------

    if command_list[0] == 'bars':
        command_type = 'bars'
    elif command_list[0] =='companies':
        command_type ='companies' 
    elif command_list[0] == 'countries':
        command_type = 'countries'
    elif command_list[0] == 'regions':
        command_type = 'regions'
    else:
        return f'Command not recognized: {command}\n'

    ### Loop over parameter list and specify each variable ------------------------------------------
    for param in parameters_list:
        if re.search("country",param):
            #Error handling if not match the regex expression
            if not bool(re.match(r"^country=[A-Z][A-Z]$",param)):
                return f'Command not recognized: {command}\n'
            else:
                where = ['Alpha2',param[-2:]]

        elif re.search("region",param):
            #Error handling if not match the regex expression
            if not bool(re.match(r"^region=[A-Z][a-z]+$",param)):
                return f'Command not recognized: {command}\n'
            where = ['Region',param[7:]]

        elif param == "sell":
            sell_source = "sell"
        elif param == "source":
            sell_source = "source"
        elif param == "ratings":
            order_by = 'Rating'
        elif param == "cocoa":
            order_by = 'CocoaPercent'
        elif param == "number_of_bars":
            order_by = 'COUNT(*)'
        elif param == "top":
            top_bottom = 'DESC'
        elif param == "bottom":
            top_bottom = 'ASC'
        elif param.isnumeric():
            num_results = param
        elif param == 'barplot':
            barplot = 'Yes'
        else:
            return f'Command not recognized: {command}\n'

    #Error handling for invalid options for each command type--------------------------------
    if (command_type == 'bars' and order_by == 'COUNT(*)'):
        return f'Command type with invalid option: {command}\n'
    if (command_type == 'companies' and sell_source != 'NA'):
        return f'Command type with invalid option: {command}\n'
    if (command_type == 'countries' and ((len(where)!=0) and (where[0] == 'Alpha2'))):
        return f'Command type with invalid option: {command}\n'
    if (command_type == 'regions' and len(where) != 0):
        return f'Command type with invalid option: {command}\n'
    else:
        ## Process command according to command types------------------------------------------
        if command_type =='bars':
            query = process_bars(where,sell_source,order_by,top_bottom,num_results)
        elif command_type =='companies':
            query = process_companies(where,order_by,top_bottom,num_results)
        elif command_type =='countries':
            query = process_countries(where,sell_source,order_by,top_bottom,num_results)
        elif command_type == 'regions':
            query = process_regions(sell_source,order_by,top_bottom,num_results)
    
    ##### Add barplot option to the function-----------------------------------------------------------------
    result = cur.execute(query).fetchall()
    conn.close()

    if barplot == 'Yes':
        plot_bar(result,command_type,order_by,top_bottom,num_results)
        message = "Lauching the browser to show the barplot..."
        return message
    else:
        return result



def load_help_text():
    with open('Proj3Help.txt') as f:
        return f.read()



# Part 2 & 3: Implement interactive prompt and plotting. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == 'exit':
            break
        if response == 'help':
            print(help_text)
            continue
        result = process_command(response)
        if "Lauching the browser to show the barplot..." not in result:
            print(show_pretty_table(result))
        else:
            print(result)


# Make sure nothing runs or prints out when this file is run as a module/library
if __name__=="__main__":
    interactive_prompt()
