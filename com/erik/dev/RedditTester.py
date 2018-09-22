import time
import praw
import json

class RedditComment(dict):
    def __init__(self, comment_string):
        dict.__init__(self, fname=comment_string)


def run():
    from boto import kinesis
    kinesis = kinesis.connect_to_region("ca-central-1")
    #stream = kinesis.create_stream("ErikSparkPOC", 1)
    start = time.time()
    reddit = praw.Reddit(client_id='gluFwvMrQLqLuA',
                         client_secret='nowLOmNuC8tS76mrc-LQUlarngw',
                         user_agent='testscript by /u/plasmatrendybot',
                         password='plasmafury10',
                         username='plasmatrendybot')
    # Subreddit name
    subreddit = reddit.subreddit('askreddit')
    comments = subreddit.stream.comments()
    escape_limit = 100000
    x = 0
    for comment in comments:
        if comment.ups > 0:
            print(comment.body, comment.ups)
            reddit_comment = RedditComment(comment.body.encode('ascii', 'ignore'))
            try:
                kinesis.put_record("ErikSparkPOC", json.dumps(reddit_comment), "partitionkey")
            except:
                print("Failed to insert to Kinesis")
        x = x + 1
        if x > escape_limit:
            break

if __name__ == '__main__':
    run()