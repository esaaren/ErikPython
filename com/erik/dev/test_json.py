import time
import praw
import json
import sys
import re
import os


def run():
    string = '{"fname": "What a nice frying pan."}'
    j = json.loads(string)['fname']
    print j
if __name__ == '__main__':
    run()