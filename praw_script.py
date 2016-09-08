import praw, time, sqlite3, datetime, requests, sys

#TODO actuall a note --zr and zm to open and close fold levels
#TODO start/endtime at end of script
#TODO conform all sql to follow same syntax for variable substitution

def handle_interrupt( function ):
    def wrapped(*args, **kwargs):
        while True:
            try:
                return ( function(*args, **kwargs) )
            except requests.exceptions.ReadTimeout:
                time.sleep(300)
                print( 'read timeout... trying again in 300s' )
                continue
            except ConnectionResetError:
                time.sleep(300)
                print( 'connection reset...trying again in 300s' )
                continue
            except KeyboardInterrupt:
                print('\nsee you space cowboy...')
                sys.exit()
            break
    return ( wrapped )

@handle_interrupt
def get_newusers (cursor, connection, r, cap=990):
    #Initialize three lists, a list of existing users, a list of banned authors
    # and an empty list to keep track of distinct new users
    sql_cmd="SELECT author FROM bots"
    cursor.execute(sql_cmd)
    botbanlist=[ x[0] for x in cursor.fetchall() ]
    sql_cmd="SELECT * FROM redditauthors"
    cursor.execute(sql_cmd)
    name_exists=[ x[0] for x in cursor.fetchall() ]
    just_added=[]

    subreddit=r.get_subreddit('columbus')
    count=1
    for comment in praw.helpers.comment_stream(r,subreddit, limit = 1000):
        author = str(comment.author)
        count+=1
        if count >= cap:
            break
        elif author in name_exists or author in botbanlist or author in just_added:
            continue
        else:
            just_added.append(author)
            cursor.execute("INSERT INTO redditauthors VALUES('{}', 99 )".format(author))
            connection.commit()
            print('\b'*len(author), end='')
            print( 'inserted ', author, flush=True)

@handle_interrupt
def row_insert(cursor,connection,redditor,commentname_dict,user):
    #Insert row by row faster using incremental logic via max(name)
    upd_ts = datetime.datetime.now()
    count=1
    for comment in redditor.get_comments(limit=None,
    params={'before':commentname_dict[user]}):
        print('\b'*len(str( count )), end='')
        print(count, end='', flush=True)
        encode_body = str(comment.body.encode('utf-8'))

        cursor.execute("INSERT INTO reddituserdata_v3 VALUES(?,?,?,?,?,?,?)",
        [str(comment.author), str(comment.link_title), str(encode_body),
        str(comment.subreddit), str(comment.created_utc), str(comment.name),
        str(upd_ts) ])
        connection.commit()

        if count == 100:
            columns=[str(comment.author) ,str(upd_ts) , str(comment.name)]
            sql_cmd="INSERT INTO commentlimit VALUES(?,?,?)"
            cursor.execute(sql_cmd,columns)
            connection.commit()
        count+=1

@handle_interrupt
def chunk_insert(cursor,connection,redditor):
    #Create a list comprehension of up to 1000 comments. Bulk Insert into sqlite3
    #2 second overhead for short inserts
    upd_ts = datetime.datetime.now()
    chunk = [(str(comment.author),str(comment.link_title),
    str(comment.body.encode('utf-8')) , str(comment.subreddit),
    str(comment.created_utc), str(comment.name), str(upd_ts))
    for comment in redditor.get_comments(limit=None)]
    print(len(chunk))
    cursor.executemany( "INSERT INTO reddituserdata_v3 VALUES(?,?,?,?,?,?,?)", chunk )
    connection.commit()

def update_post_frequency(cursor,connection):
    now = datetime.datetime.utcnow().timestamp()
    sql_cmd="REPLACE into redditauthors " \
            "SELECT author,count(*)/(({}-min(created_ts))/86400) " \
            "FROM reddituserdata_v3 " \
            "GROUP BY author".format(now)
    cursor.execute(sql_cmd)
    connection.commit()

def frequency_scheduler(cursor, connection, r):
    thetime=datetime.datetime.now()
    hour,day = thetime.hour,thetime.day
    #regularly schedule new user inserts as well as frequent posters
    #note that new users do not yet have a unique row in postfrequency
    if hour in (0,12):
        get_newusers(cursor, connection, r)
        sql_cmd="SELECT author FROM redditauthors WHERE postrate > 10 "
    #schedule infrequent posters every 2 or 7 days
    if hour == 1:
        if day%2 == 0:
            sql_cmd="SELECT author from redditauthors where postrate <= 10 and postrate > 1"
    if hour == 2:
        if day%7 == 0:
            sql_cmd="SELECT author from redditauthors where postrate <= 1"

    cursor.execute(sql_cmd)
    users_queue=cursor.fetchall()
    users_queue=[x[0] for x in users_queue]
    return(users_queue)

def fetch_latest_comment(cursor):
    sql_cmd="SELECT author, max(name) FROM reddituserdata_v3 group by author"
    cursor.execute(sql_cmd)
    maxcreated=cursor.fetchall()
    commentname_dict = { x[0]:x[1] for x in maxcreated }
    return(commentname_dict)

def remove_user(cursor,connection,user):
    print( 'reddit user account was likely deleted...')
    print( 'username will be removed from initial query...')
    sql_cmd="DELETE FROM redditauthors where author='{}'".format(user)
    cursor.execute(sql_cmd)
    connection.commit()

def main():
    #Create user_agent per PRAW standards, establish local db access
    print(  datetime.datetime.now() )
    print( "\nstarting..." )
    user_agent = 'last 1000 comments for users from r/Columbus by /u/undelimited'
    r = praw.Reddit(user_agent=user_agent)
    connection = sqlite3.connect('/home/dan/reddit.db')
    cursor = connection.cursor()

    users_queue=frequency_scheduler(cursor, connection, r)
    commentname_dict = fetch_latest_comment(cursor)

    #Prepare the main while loop
    #Each loop will check if the user exists and either bulk insert if they do not
    #or row by row insert if they do
    usernum=0
    userstotal = len(users_queue)
    while users_queue:
        usernum+=1
        user = str(users_queue.pop())
        redditor = r.get_redditor(user)
        print('user ', usernum,'/', userstotal,' ==>> ', redditor, sep='')

        #Check if user exists in our sqlite3 db
        if commentname_dict.get(user) is None:
            #INSERT LOGIC
            chunk_insert(cursor,connection,redditor)
        #UPDATE LOGIC
        else:
        #useraccounts may have been deactivated. Cannot take action based on username dynamically inside
        #wrapper decorator
            try:
                row_insert(cursor,connection,redditor,commentname_dict,user)
            except praw.errors.NotFound:
                remove_user(cursor, connection, user)

    print('finished loading records...\n')
    print('inserting into postfrequency table...\n')
    # Post frequency will be used to bucket activity levels by user and select
    # them at an appropriate interval
    update_post_frequency(cursor, connection)
    cursor.close()

if __name__ == '__main__':
    main()
