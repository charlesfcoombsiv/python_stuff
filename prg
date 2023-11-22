def pivot_dynamic_snflk(indata,aggregate_func, value_var, pivot_var, outdata, cur, suffix=''):
    """
    pivot_dynamic_snflk
    Since Snowflake requires stating the distinct values to pivot on, this function finds those values and the names 
    of the other columns in the table automatically.
    

    Parameters:
            indata               : (string) Name of the Snowflake table to pivot.
                                            Needs to include database and schema unless specify database and schema in cursor.
                                            Can also be a select statment in parenthesis, e.g.
                                            (SELECT patient_token_2, month, allowed_amount FROM amounts)
            
            aggregate_func       : (string) Function for the pivot, e.g. sum, max, min.
            
            value_var            : (string) Column that has the values that will become columns in the pivot.
                                            These values need to be ready to become column names, so no spaces or special characters.
            
            pivot_var            : (string) Column that has the values that will be aggregated in the pivot.
            
            outdata              : (string) Name of the Snowflake table that is created from the mapping.
                                            Needs to include database, schema, and table					
            
            cur                  : (cursor) Cursor with the Snowflake connection to the warehouse and can also specific database and schema.
                
            suffix               : (string) (not required) Suffix to add to the value_var values that will be used
                                            to name the pivoted columns.
                      
      
    Returns:
            df                  : (dataframe) Dataframe with the values of value_var and the corresponding column
                                              names that they become. This is useful for automating table creation
                                              where value_var values may change.

    Example: 
        
        df_coco_test_vars = pivot_dynamic_snflk(indata = '(SELECT DISTINCT patient_token_2, descr, descr_index_flag FROM coco_dates_flagged)',
                                             aggregate_func = 'max',value_var = 'descr',pivot_var = 'descr_index_flag', outdata='coco_test',
                                             cur=cur_prj,suffix = '_flag')
        
    Author: Charles Coombs - 2021/09/16

    Updates:


    """
    # Convert to lowercase for naming consistency
    pivot_var = pivot_var.lower()
    value_var = value_var.lower()
    
    # Find all the value_var values to create the column names later
    df = cur.execute("SELECT DISTINCT {} FROM {}".format(value_var, indata)).fetch_pandas_all()
    
    # Make column names all lowercase
    df.columns = map(str.lower, df.columns)
    
    # Wrap the column values with '' so can be selected later in pivot
    df[value_var + "_p"] = "'" + df[value_var] + "'"
    
    # Convert to string for use in pivot later
    value_var_values = ",".join(df[value_var + "_p"])
    
    # Add suffix to outdata column names if specified
    if suffix:
        df[value_var + "_outdata"] = df[value_var] + suffix
    else:
        df[value_var + "_outdata"] = df[value_var]
        
    # Find all the columns from indata
    df_indata = cur.execute("SELECT * FROM {} LIMIT 0".format(indata)).fetch_pandas_all()
    indata_vars = [str(x).lower() for x in df_indata.columns]
    
    # Create list of outdata columns for the matching pivot.
    # pivot_var and value_var are not in the outdata.
    outdata_vars = ",".join([x for x in indata_vars if x not in [pivot_var, value_var]] + list(df[value_var + "_outdata"]))
    
    #######
    # Pivot
    #######
    cur.execute(""" CREATE OR REPLACE TABLE {outdata} AS
                        SELECT *
                        FROM {indata}
                        PIVOT ({aggregate_func}({pivot_var}) FOR {value_var} IN ({value_var_values})) 
                            AS p ({outdata_vars})
                    
                    """.format(outdata=outdata, indata=indata, aggregate_func = aggregate_func, pivot_var=pivot_var,
                                value_var =value_var, value_var_values = value_var_values, outdata_vars = outdata_vars))
    
    return df
    
    
# Cols to include need to be in the order as appeared in the table
# cur = cur_prj
# indata = '(SELECT DISTINCT patient_token_2, desc, desc_12m_flag FROM coco_dates_flagged)'
# aggregate_func = 'max'
# value_var = 'DESC'
# pivot_var = 'desc_12m_flag'
# outdata= 'coco_test'
# suffix = '_flag'

# df_outdata = pivot_dynamic_snflk(indata = '(SELECT DISTINCT patient_token_2, desc, desc_12m_flag FROM coco_dates_flagged)',
#                                              aggregate_func = 'max',value_var = 'DESC',pivot_var = 'desc_12m_flag', outdata='coco_test',
#                                              cur=cur_prj,suffix = '_flag')
