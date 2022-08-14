import tweepy


def main():
    tester = TweepyTester()

    tester.test_home_timeline()

    tester.outf.close()


class TweepyTester:
    def __init__(self):
        outf_name = "test_tweepy"
        self.outf = open(f"{outf_name}.txt", "w")
    
    def test_home_timeline(self):
        print(f"test_home_timeline output -->\n", file=self.outf)
        auth = tweepy.OAuth1UserHandler(
            consumer_key, consumer_secret, access_token, access_token_secret
        )
        api = tweepy.API(auth)

        public_tweets = api.home_timeline()
        for tweet in public_tweets:
            print(tweet.text, file=self.outf)


if __name__ == "__main__": main()
