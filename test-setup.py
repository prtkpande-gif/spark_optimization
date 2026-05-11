
import os, sys

checks = {
    'Python version':    sys.version,
    'JAVA_HOME':         os.environ.get('JAVA_HOME', 'MISSING'),
    'PYSPARK_PYTHON':    os.environ.get('PYSPARK_PYTHON', 'MISSING'),
    'SPARK_LOCAL_IP':    os.environ.get('SPARK_LOCAL_IP', 'MISSING'),
}

print('--- Environment Check ---')
for k, v in checks.items():
    status = 'MISSING' if v == 'MISSING' else 'OK'
    print(f'  [{status}]  {k}: {v}')

print()
print('--- Package Check ---')
packages = ['pyspark', 'delta', 'kafka', 'boto3', 'faker']
for pkg in packages:
    try:
        __import__(pkg)
        print(f'  [OK]  {pkg}')
    except ImportError:
        print(f'  [MISSING]  {pkg}')