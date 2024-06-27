import boto3
import pickle
import pandas as pd

def load_pickle_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response['Body'].read()
        return pickle.loads(body)
    except Exception as e:
        print(f"Error reading {file_key} from bucket {bucket_name}: {e}")
        return None

def lambda_handler(event, context):
    bucket_name = 'layerk'  # S3バケット名
    file_key = '2024aws.pkl'  # S3のファイルキー
    
    # S3からファイルを読み込んでデシリアライズ
    df_moto = load_pickle_from_s3(bucket_name, file_key)
    
    if df_moto is not None:
        print(f"Successfully loaded DataFrame with shape: {df_moto.shape}")
        return {
            'statusCode': 200,
            'body': 'Successfully loaded pickle file from S3.'
        }
    else:
        return {
            'statusCode': 500,
            'body': 'Failed to load pickle file from S3.'
        }
