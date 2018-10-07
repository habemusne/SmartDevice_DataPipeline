
#Script to copy data from s3 to redshift running on one of the instances on spark cluster
python /home/ubuntu/copyDataFromS3ToRedshift.py --schema public --dbname dev --port 5439 --user awsuser --password <password> --host_url <redshift-endpoint-url> --filePath <s3-bucket-file-path> --awsAccessKey <access-key> --awsSecretKey <aws-secret-key>
