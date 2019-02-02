import os
import psycopg2
import boto3

s3 = boto3.client('s3')
iam_role = os.environ["IAM_ARN_ROLE"]
db_database = os.environ["REDSHIFT_DATABASE"]
db_user = os.environ["REDSHIFT_USER"]
db_password = os.environ["REDSHIFT_PASSWORD"]
db_port = os.environ["REDSHIFT_PORT"]
db_host = os.environ["REDSHIFT_ENDPOINT"]

def handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    try:
        conn = psycopg2.connect("dbname=" + db_database
                                + " user=" + db_user
                                + " password=" + db_password
                                + " port=" + db_port
                                + " host=" + db_host)

        conn.autocommit = True
        cur = conn.cursor()

        if is_file_already_copied(bucket, cur, key):
            result = 'Already uploaded {}/{}, skipped.'.format(bucket, key)
        else:
            copy_to_redshift(bucket, cur, key)
            result = 'Successfully uploaded {}/{}'.format(bucket, key)

        cur.close()
        conn.close()

        print(result)
        return result
    except Exception as e:
        print(e)
        print('Error saving {}/{}'.format(bucket, key))
        raise e


def copy_to_redshift(bucket, cur, key):
    query = get_copy_query(bucket, key)
    cur.execute(query)


def is_file_already_copied(bucket, cur, key):
    uploaded = already_uploaded_query(bucket, key)
    cur.execute(uploaded)
    count = cur.fetchone()
    return count[0] > 0


def get_copy_query(bucket, key):
    table_name = 'raw_cdrs'
    sql = '''
    copy {} 
    from 's3://{}/{}' 
    credentials 'aws_iam_role={}'
    escape
    ACCEPTINVCHARS
    delimiter ';' 
    timeformat 'YYYY-MM-DD HH:MI:SS' 
    IGNOREHEADER 1 
    REMOVEQUOTES 
    gzip;
    '''.format(table_name, bucket, key, iam_role)
    return sql


def already_uploaded_query(bucket, key):
    sql = "select count(1) as total from stl_load_commits where filename = 's3://{}/{}';".format(bucket, key)
    return sql
