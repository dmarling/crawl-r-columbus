import praw
import time
user_agent = 'praw explorer 1.0 by /u/undelimited'
r = praw.Reddit(user_agent=user_agent)

donefile=open('/home/dan/Projects/crawl-r-cbus/storedids.txt','r')
already_done=donefile.readlines()
donefile.close()

print(already_done, 'first already_done print')

subreddit=r.get_subreddit('columbus')
doing_now=[]
datafile=open('/home/dan/Projects/crawl-r-cbus/datafile.txt','a')
for post in subreddit.get_hot():
    if post.id+'\n' not in already_done:
        op_text = (post.selftext.lower(), post.author, post.domain, post.title)
        datafile.write(str(op_text)+'\n')  
        doing_now.append(post.id)
datafile.close()
donefile=open('/home/dan/Projects/crawl-r-cbus/storedids.txt','a')

for x in doing_now:
    donefile.write(x+'\n')
donefile.close()
print(already_done, 'end')

#subreddit=r.get_subreddit('columbus')
#comments=praw.helpers.comment_stream(r,subreddit,limit=10,verbosity=1)
#mylist={}
#for x in comments:
#	print(x.id)
#	print(type(x))
#	print(x)
#	print(x.author)
#	print(x.domain)
#	time.sleep(1)
#	mylist[x.author] = x
#	#TODO never reaches final print statement
#	#TODO from the comment see if you can access the post 'title'
#	#ideally we have username,subreddit,post,comment
#
#print(mylist)



#not tested
#submission=r.get_submission(submission_id='32rc86')
#submission.load_more_comments(limit=None, threshold=1)
#commentlist = praw.helpers.flatten_tree(submission.comments)


