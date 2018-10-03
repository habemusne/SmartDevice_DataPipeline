import sys
import time
import psycopg2

#Get the required parameters
#args = sys.argv
#schema = args[2]
#dbname = args[3]
#port =  args[4]
#user =  argv[5]
#password = argv[6]
#host_url = argv[7]
#file_path = argv[8]
#aws_access_key_id = args[9]
#aws_secret_access_key = args[10]

def main():
    #Make the connection string to connect to redshift
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
        .format(dbname,port,user,password,host_url)  
    
    #SQL command to be executed to copy the data from S3 to redshift
    sql="""copy {}.{} from '{}'\
        credentials \
        'aws_access_key_id={};aws_secret_access_key={}' \
        DELIMITER ',' ACCEPTINVCHARS EMPTYASNULL ESCAPE COMPUPDATE OFF;commit;"""\
        .format(schema, table, file_path, aws_access_key_id, aws_secret_access_key)

    try:
        con = psycopg2.connect(conn_string)
        print("Connection Successful!")
    except:
        print("Unable to connect to Redshift")

    cur = con.cursor()
    try:
        cur.execute(sql)
        print("Copy Command executed successfully")
    except:
        print("Failed to execute copy command")
    con.close() 

if __name__ == "__main__":
    main()
