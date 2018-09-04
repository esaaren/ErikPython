import json
from faker import Faker


class User(dict):
    def __init__(self, fname):
        fake = Faker()
        name = fake.name()
        dict.__init__(self, fname=name)


def run():
    from boto import kinesis
    kinesis = kinesis.connect_to_region("ca-central-1")
    #stream = kinesis.create_stream("ErikDemo", 1)


    i = 0

    for i in xrange(10):
        user = User("Demo")
        #print(json.dumps(user))
        print("Did I make it here?")
        kinesis.put_record("ErikDemo", json.dumps(user), "partitionkey")


if __name__ == '__main__':
    run()