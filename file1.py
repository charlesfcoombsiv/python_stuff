def icd_code_flags_snflk(indata, indata_code_var, indata_code_type_var, outdata, map, cur, sheet_name=0, map_descr_var="descr2", xwhere=''):
    """icd_code_flags_snflk
       Maps the codes and descriptions from an Excel file to a Snowflake table
    
    Paramters:
            indata               : (string) Full name of the Snowflake table to map against.
                                            Needs to include database, schema, and table.
            
            indata_code_var      : (string) Name of diagnosis column on Snowflake table
            
            indata_code_type_var : (string) Name of diagnosis code type column on Snowflake table for ICD9 vs 10
            
            outdata              : (string) Name of the Snowflake table that is created from the mapping.
                                            Needs to include database, schema, and table.
            
            map                  : (string) Location of the Excel file with the map.
											The map file needs to follow a specific format.
											See 'codelist_test.xlsx' in the program folder for the format.
                                 
            sheet_name           : (string) Sheet name of from the Excel file with the map.
                                            Deafaut is 0 which picks the first sheet.
											
			map_descr_var		 : (string) Name of the column from the map file to use to for the flag description.							
            
            cur                  : (cursor) Cursor with the Snowflake connection to the warehouse but not a specific database or schema.
                
            xwhere               : (string) (not required) Additional where statments to use when mapping the codes.
											Useful to limit your indata table to specific date.
											Needs to start with 'AND'.
    
    Returns:
            The function makes a table designated by outdata. The outdata table has all the columns from
            indata and 3 extra columns MAP_CODE, MAP_CODE_TYPE, and MAP_DESCR hat corresponds to the
            regex or range value created from indata_code_var and the values from indata_code_type_var
            and map_descr_var.
            It also now returns 2 dataframes: the processed code list and regex list used for the mapping.
    
    Example: icd_flag_codelist, icd_flag_regex_codelist= icd_code_flags_snflk(indata='WHAREHOUSE.DATABASE.TABLEIN',
                                  outdata='WHAREHOUSE.DATABASE.TABLEOUT',
                                  indata_code_var='dx', indata_code_type_var='icd_type',
                                  map="C:\\python_packages\\transform\\codelist_test.xlsx", 
                                  cur=cur, xwhere="AND claim_date >= '2017-01-01'")
    
    Author: Charles Coombs - 2021/07/16
    
    Update History:
            2021-07-30 (Charles Coombs)
                Changed it to be able to handle individual codes and ranges in the same cell.
                
            2021-08-05 (Charles Coombs)
                Added ability to specify sheet from the Excel map.
            
            2021-08-11 (Charles Coombs)
                Added icd code type to code lookup.
                
            2021-08-16 (Charles Coombs)
                Fixed bug where'desc','desc2' was being used instead of map_desc_var .
                Added return of the 2 dataframes used in the mapping.
                
            2021-08-18 (Charles COombs)
            Fixed bug where code_type_list was filled with the range instead of the code type.
            
            2021-09-22 (Charles Coombs)
            Change description abreviation of 'desc' to 'descr'.
            
            2021-11-22 (Charles Coombs)
            Add Python varaible names inline to the SQL code so it was more readable.
            
            2022-02-11 (Charles Coombs)
            Only choses ICD code_type so doesnt also bring in CPT codes in the same map.
                
    
    """
    
    import pandas as pd
    import numpy as np
    
    #####################################################
    # Prep codelist
    #####################################################
    # Dataframe is named comor_codes but a better description would be any diagnosis codes.
    df_comor_codes = pd.read_excel(map, sheet_name= sheet_name, engine='openpyxl')
    
    
    ##########
    # Only select ICD codes
    ##########
    df_comor_codes = df_comor_codes[df_comor_codes['code_type'].str.contains('ICD', case=False, regex=False)]
    

    ##########
    # Match icd type map value to database value
    ##########
    df_comor_codes['code_type'] = np.where(df_comor_codes['code_type'].str.contains('9'),'9',
                                 np.where(df_comor_codes['code_type'].str.contains('10'),'10',df_comor_codes['code_type']))
    
    ##########
    # Clean-up column
    ##########
    # Convert to string because sometimes get set as mix of number of string
    
    df_comor_codes["code"] = df_comor_codes["code"].astype("string")
    df_comor_codes["code_orig"] = df_comor_codes["code"]
    
    # Remove blank spaces
    df_comor_codes["code"] = df_comor_codes["code"].str.replace(" ","")
    
    ##########
    # Explode codes into different rows
    ##########
    # Need to do this in case one cell has both individual codes and a range
    df_comor_codes["code_array"] = df_comor_codes["code"].str.split(',')
    df_comor_codes = df_comor_codes.explode('code_array')
    
    ##########
    # Create column to hold code ranges
    ##########
    df_comor_codes["code_range"] = np.where(df_comor_codes["code_array"].str.contains("-"), df_comor_codes["code_array"], None)
    
    ##########
    # Create dataframe for non code ranges
    ##########
    # Made another dataframe for just regex so that the regex could be grouped together for the same descr,
    # and so there was df_comor_codes that had both the ranges and regex together as a summary.
    # Snowflake Regex: The function implicitly anchors a pattern at both ends 
    # (i.e. '' automatically becomes '^$', and 'ABC' automatically becomes '^ABC$'). 
    # To match any string starting with ABC, the pattern would be 'ABC.*'
    # Replace 'x' for '.' as wild card for any character.
    
    df_comor_codes["code_regex"] = np.where(~df_comor_codes["code_array"].str.contains("-"), 
                                                df_comor_codes["code_array"].str.replace("x","."), None)
    # .str.replace(",",".*|").
    
    df_comor_codes["code_regex"] = df_comor_codes["code_regex"] + ".*"
    
    # Group them back together since they were exploded
    df_regex_codes =  df_comor_codes[df_comor_codes['code_regex'].notna()].groupby(['code_type',map_descr_var])['code_regex']\
                        .apply('|'.join).reset_index()
    
    ######################################################
    # Create outdata
    ######################################################
    cur.execute("""CREATE OR REPLACE TABLE {} LIKE {}""".format(outdata, indata))
    cur.execute("""ALTER TABLE {} ADD (\"MAP_CODE\" varchar,\"MAP_CODE_TYPE\" varchar,\"MAP_DESCR\" varchar)""".format(outdata))
    
    ######################################################
    # Find regex flags
    ######################################################
    
    #######
    # Find regex codes and descriptons then loop through them and search
    ######
    code_list = list(df_regex_codes["code_regex"])
    code_type_list = list(df_regex_codes["code_type"])
    descr_list = list(df_regex_codes[map_descr_var])
    
    for i, val in enumerate(code_list):
        code = val
        code_type = code_type_list[i]
        descr = descr_list[i]
        
        cur.execute(""" INSERT INTO {outdata}
                    SELECT * , '{code}' AS map_code, '{code_type}' AS map_code_type, '{descr}' AS map_descr
                    
                    FROM {indata}
                    
                    WHERE {indata_code_var} REGEXP ('{code}') AND contains({indata_code_type_var}, '{code_type}') {xwhere}
                    
                    """.format(outdata=outdata, code=code, code_type=code_type, descr=descr, indata=indata, 
                                indata_code_var=indata_code_var, indata_code_type_var=indata_code_type_var, xwhere=xwhere))
                    
    del code_type_list, descr_list
    ######################################################
    # Find range flags
    ######################################################
    range_list = list(df_comor_codes[df_comor_codes["code_range"].notna()]["code_range"])
    code_type_list = list(df_comor_codes[df_comor_codes["code_range"].notna()]["code_type"])
    descr_list = list(df_comor_codes[df_comor_codes["code_range"].notna()][map_descr_var])
    
    for i, val in enumerate(range_list):
        code = val
        code_type = code_type_list[i]
        descr = descr_list[i]
        
        range_low = code.split('-')[0]
        # 170-182 -> '182' becomes '182ZZZZ' or it will not find something like '1822'
        range_high = code.split('-')[1].ljust(7,'Z')
        
        # The between statement will find the right codes even if range1 is '1411' and range2 is '239'.
        cur.execute(""" INSERT INTO {outdata}
                    SELECT * , '{code}' AS map_code, '{code_type}' AS map_code_type, '{descr}' AS map_descr
                    
                    FROM {indata}
                    
                    WHERE {indata_code_var} BETWEEN '{range_low}' AND '{range_high}' AND contains({indata_code_type_var},'{code_type}') {xwhere}
                    
                    """.format(outdata=outdata, code=code, code_type=code_type, descr=descr, indata=indata, 
                                indata_code_var=indata_code_var, range_low=range_low, range_high=range_high, 
                                indata_code_type_var=indata_code_type_var, xwhere=xwhere))
                    
    del code_type_list, descr_list
    
    return df_comor_codes, df_regex_codes
