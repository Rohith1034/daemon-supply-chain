import sys
sys.path.insert(0, r'C:\Users\rohit\PycharmProjects\daemon-supply-chain')
from tests.validation.validator_utils import generate_validation_report
report = generate_validation_report(output_path=r'C:\Users\rohit\PycharmProjects\daemon-supply-chain\output\validation_report.json')
print(report['summary'])
for item in report['edge_cases']:
    print(item)
