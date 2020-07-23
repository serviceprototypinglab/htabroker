# Test function for Pulsar

import os

def checkenv():
    f = open("/tmp/htaproc.debug", "w")
    f.write(str(os.environ))
    f.close()

def process(inputmsg):
    checkenv()
    return inputmsg + "-PROCESSED-BY-PULSAR"

"""
# cat /tmp/htaproc.debug
{'LANG': 'C.UTF-8', 'TERM': 'xterm', 'LOG_FILE': u'/tmp/functions/public/default/htaproc/htaproc-0.log', 'JAVA_VERSION': '8u252', 'PULSAR_HOME': '/pulsar', 'PYTHONPATH': '${PYTHONPATH}:/pulsar/instances/deps', 'OLDPWD': '/pulsar', 'HOSTNAME': '8f2d80fc1be3', 'JAVA_BASE_URL': 'https://github.com/AdoptOpenJDK/openjdk8-upstream-binaries/releases/download/jdk8u252-b09/OpenJDK8U-jdk_', 'SHLVL': '0', 'PWD': '/pulsar', 'JAVA_URL_VERSION': '8u252b09', 'PATH': '/usr/local/openjdk-8/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin', 'JAVA_HOME': '/usr/local/openjdk-8', 'HOME': '/root', 'PULSAR_ROOT_LOGGER': 'INFO,CONSOLE'}root@8f2d80fc1be3:/pulsar
"""
