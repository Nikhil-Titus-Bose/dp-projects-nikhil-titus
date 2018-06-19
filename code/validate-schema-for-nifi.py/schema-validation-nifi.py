import sys
import getopt
import json
import importlib
from collections import namedtuple
import os
import tempfile
import subprocess
import json
import sys
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

sys.path.append('/opt/nifi/data/validateudcscripts/1/site-packages')
is_success = True
validator_dict = {}

ValidatorInfo = namedtuple(
    'ValidatorInfo', ['validator_object', 'validation_exception'])


class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        global is_success
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        # Do something with text here

        out = validateFromString(text)
        results = out
        print results
        # with open('/Users/nikhiltitus/work/repos/svc-data-collector/service/data_collector/resources/somefile.txt', 'w') as the_file:
        #     the_file.write(json.dumps(results))
        if (results['summary'].startswith('Passed')):
            is_success = True
        else:
            is_success = False
        results_string = json.dumps(results)+'\n'+text
        outputStream.write(results_string.encode('utf-8'))


def getModule(modulePath):
    try:
        my_module = importlib.import_module(modulePath)
    except:
        print >> sys.stderr, 'Error: Could not load module ' + modulePath
        return None
    else:
        return my_module


def getClassByName(module, className):
    my_class = getattr(module, className)
    return my_class


def getValidatorInfoObject(module, class_name, payload_type=None, payload_protocol_version=None):
    global validator_dict

    validator_key = module.__name__ + '_' + \
        class_name + str(payload_protocol_version)

    if validator_key not in validator_dict:
        clazz = getClassByName(module=module, className=class_name)
        clazz_instance = clazz()

        package_name = getPackageName(payload_type)
        validation_exception_class = getClassByName(getModule(package_name + '.validator.validator'),
                                                    'ValidationException')

        validator_info = ValidatorInfo(validator_object=clazz_instance,
                                       validation_exception=validation_exception_class)
        validator_dict[validator_key] = validator_info

    return validator_dict[validator_key]


def validate(msgDict, payloadType=None, payloadProtocolVersion=None):
    results = {
        'originalMessage': msgDict
    }

    # If payloadType is not specified, look it up from msgDict
    if payloadType is None:
        payloadType = getPayloadType(msgDict)

    # If payloadProtocolVersion is not specified, look it up from msgDict
    if payloadProtocolVersion is None:
        payloadProtocolVersion = getPayloadProtocolVersion(msgDict)

    packageName = getPackageName(payloadType)

    mod = getModule(packageName + '.validator.message_validator')

    if mod is None:
        results['error'] = 'Package ' + packageName + \
            ' could not be found. Make sure envelope.payloadType is set.'
        return results

    # ValidatorClass = getClassByName(mod, 'MessageValidator')
    # validator = ValidatorClass()
    validator_info = getValidatorInfoObject(module=mod,
                                            class_name='MessageValidator',
                                            payload_type=payloadType,
                                            payload_protocol_version=payloadProtocolVersion)
    # ValidationException = getClassByName(getModule(packageName + '.validator.validator'), 'ValidationException')

    validator = validator_info.validator_object
    ValidationException = validator_info.validation_exception

    try:
        validator.validate(msgDict)
    except ValidationException as exception:
        results = {
            'summary': 'Failed validation check',
            'details': list(map(lambda x: x.replace('\n', ' - '), exception.errors)),
            'originalMessage': msgDict
        }
        return results
    else:
        results = {
            'summary': 'Passed validation check',
            'originalMessage': msgDict
        }

    return results


def validateFromStdin():
    try:
        input = json.load(sys.stdin)
    except KeyboardInterrupt:
        print "\nCTRL-C detected. Exiting."
        sys.exit()
    except:
        return {
            'summary': 'Failed validation check',
            'details': ['Invalid JSON']
        }
    else:
        return validate(input)


def validateMultiFromStdin():
    while True:
        validateFromStdin()


def validateFromFile(filepath):
    try:
        f = open(filepath, 'r')
    except:
        print >> sys.stderr, 'Could not open file ' + filepath
        sys.exit()
    else:
        try:
            input = json.loads(f.read())
        except:
            return {
                'summary': 'Failed validation check',
                'details': ['Invalid JSON']
            }
        else:
            return validate(input)


def validateFromString(inputString):
    try:
        input = json.loads(inputString)
        output = validate(input)
        return output
    except:
        return {
            'summary': 'Failed validation check',
            'details': ['Invalid JSON']
        }


def validateMultiFromFile(filepath):
    results = []
    try:
        f = open(filepath, 'r')
    except:
        print >> sys.stderr, 'Could not open file ' + filepath
        sys.exit()
    else:
        lineNumber = 0
        for line in f:
            try:
                lineNumber += 1
                if line == '':
                    continue
                input = json.loads(line)
            except:
                results.append({
                    'summary': 'Failed validation check',
                    'details': ['Invalid JSON at line {}'.format(lineNumber)]
                })
            else:
                results.append(validate(input))

    return {
        'results': results
    }


def getItemValidatorClassName(type):
    if type is None:
        return None
    return ''.join(x.capitalize() for x in type.split('-')) + 'EventValidator'


def getItemValidatorModuleName(type):
    if type is None:
        return None
    return type.replace('-', '_') + '_event_validator'


def getPayloadType(dict):
    if dict is not None:
        envelope = dict.get('envelope')
        if envelope is not None:
            return envelope.get('payloadType')
    return None


def getPayloadProtocolVersion(dict):
    if dict is not None:
        envelope = dict.get('envelope')
        if envelope is not None:
            return envelope.get('payloadProtocolVersion')
    return None


def getPackageName(type):
    if type is not None:
        return 'data_collection_' + type + '_sdk'
    else:
        return 'data_collection_sdk'


def main():
    global is_success
    flowFile = session.get()
    if(flowFile != None):
        session.write(flowFile, PyStreamCallback())
    if (is_success):
        session.transfer(flowFile, REL_SUCCESS)
    else:
        session.transfer(flowFile, REL_FAILURE)

    # print json.dumps(validateFromString(inputMessage), indent=4, separators=(',', ': '))


main()
