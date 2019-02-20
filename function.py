import os
import boto3
import datetime
from boto3.session import Session


def lambda_handler(event, context):
    dynamodb = prepare_dynamodb()
    users = fetch_user(dynamodb)
    users = delete_past_programs(users)
    update_user(dynamodb, users)
    # print(users)


def prepare_dynamodb():
    if os.environ.get('EXEC_ENV') == 'TEST':
        session = Session(profile_name='local-dynamodb-user')
        dynamodb = session.resource('dynamodb')
    else:
        dynamodb = boto3.resource('dynamodb')
    return dynamodb


def fetch_user(dynamodb):
    # return [
    #     {'NotifyToken': 'www', 'Programs': {
    #         'Bolivia': [
    #             {'Date': '2019/02/18', 'Title': 'Bolivia01'},
    #             {'Date': '2019/02/19', 'Title': 'Bolivia02'},
    #             {'Date': '2019/02/20', 'Title': 'Bolivia03'},
    #             {'Date': '2019/02/21', 'Title': 'Bolivia04'},
    #             {'Date': '2019/02/22', 'Title': 'Bolivia05'},
    #         ],
    #         'Rotterdam': [
    #             {'Date': '2019/02/18', 'Title': 'Rotterdam01'},
    #             {'Date': '2019/02/19', 'Title': 'Rotterdam02'},
    #             {'Date': '2019/02/20', 'Title': 'Rotterdam03'},
    #             {'Date': '2019/02/21', 'Title': 'Rotterdam04'},
    #             {'Date': '2019/02/22', 'Title': 'Rotterdam05'},
    #         ],
    #     }},
    #     {'NotifyToken': 'yyy', 'Programs': {
    #         'Japan': [
    #             {'Date': '2019/02/18', 'Title': 'Japan01'},
    #             {'Date': '2019/02/19', 'Title': 'Japan02'},
    #             {'Date': '2019/02/20', 'Title': 'Japan03'},
    #             {'Date': '2019/02/21', 'Title': 'Japan04'},
    #             {'Date': '2019/02/22', 'Title': 'Japan05'},
    #         ],
    #         'Amsterdam': [
    #             {'Date': '2019/02/18', 'Title': 'Amsterdam01'},
    #             {'Date': '2019/02/19', 'Title': 'Amsterdam02'},
    #             {'Date': '2019/02/20', 'Title': 'Amsterdam03'},
    #             {'Date': '2019/02/21', 'Title': 'Amsterdam04'},
    #             {'Date': '2019/02/22', 'Title': 'Amsterdam05'},
    #         ],
    #     }}
    # ]
    table = dynamodb.Table('User')
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


def delete_past_programs(users):
    for user in users:
        new_programs = {}
        for search_word, program_list in user['Programs'].items():
            new_program_list = []
            for program in program_list:
                if not is_past(program['Date']):
                    new_program_list.append(program)
            if new_program_list:
                new_programs[search_word] = new_program_list
        user['Programs'] = new_programs
    return users


def is_past(program_date_str):
    today = datetime.date.today()
    program_date = datetime.datetime.strptime(program_date_str, "%Y/%m/%d").date()
    return program_date < today


def update_user(dynamodb, users):
    for user in users:
        table = dynamodb.Table('User')
        response = table.update_item(
            Key={
                'UserId': user['UserId']
            },
            UpdateExpression="set Programs = :p",
            ExpressionAttributeValues={
                ':p': user['Programs']
            },
            ReturnValues="UPDATED_NEW"
        )
    return users
