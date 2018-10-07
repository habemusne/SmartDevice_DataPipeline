import sys
import time
import psycopg2
import argparse

#Get the required parameters
parser = argparse.ArgumentParser(description='Copy data from S3 to Redshift')
parser.add_argument('--schema', type=str, default='public',
                    help="schema of the database")
parser.add_argument('--dbname', type=str, default='dev',
                    help="Name of the database")
parser.add_argument('--port', type=int, default=5439,
                    help="port to connect to the database")
parser.add_argument('--user', type=str, default='awsuser',
                    help='name of the user provided at the time of redshift cluster creation')
parser.add_argument('--table', type=str, default='rawData',
                    help='name of the table created in redshift cluster')
parser.add_argument('--password', type=float, default=<password>,
                    help='password for the provided user at the time of redshift cluster creation ')
parser.add_argument('--host_url', type=str, default='redshift-cluster-1.cy68m2eaezzl.us-east-1.redshift.amazonaws.com',
                    help="URL of the connection. Can be found in redshift cluster information on AWS")
parser.add_argument('--filePath', type=str, default='s3://anshu-insight/rawData_<currentData>/',
                    help='file path from where data is to be copied. Provide current date in format <YYYY-MM-DD> with folder name')
parser.add_argument('--awsAccessKey', type=str, default=<aws-access-key>,
                    help="Provide the access key id AWS provided on IAM role creation")
parser.add_argument('--awsSecretKey', type=str, default=<aws-secret-key>,
                    help="Provide the secret key id AWS provided on IAM role creation")
args = parser.parse_args()


def main():
    #Make the connection string to connect to redshift
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
                   .format(args.dbname,ars.port,ars.user,ars.password,ars.host_url)  
    
    #SQL command to be executed to copy the data from S3 to redshift
    sql="""copy {}.{} from '{}'\
            credentials \
            'aws_access_key_id={};aws_secret_access_key={}' \
            DELIMITER ',' ACCEPTINVCHARS EMPTYASNULL ESCAPE COMPUPDATE OFF;commit;"""\
            .format(args.schema, args.table, args.filePath, args.awsAccessKey, args.awsSecretKey)

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
