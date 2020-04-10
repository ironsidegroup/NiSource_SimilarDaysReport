import json
from handlers import S3Handler, ReportHandler


def lambda_handler(event, context):
    s3 = S3Handler('isg-nisource-test-bucket')
    filenames = s3.stage()

    r = ReportHandler(filenames['report'], filenames['daily'], 'historical')
    report = r.generate(5, logging=True)

    s3.unstage(report, filenames)

    return {
        'statusCode': 200,
        'body': json.dumps('Report was generated successfully.')
    }
