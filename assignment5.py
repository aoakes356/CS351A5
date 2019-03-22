import pymysql
import json
import csv
import datetime
import sys

args = sys.argv
arglen = len(args)
if(arglen < 3):
    print("Not enough arguments, exiting")
    sys.exit()
conn = pymysql.connect(host='localhost', port=3306, user=args[1], passwd=args[2])

cur = conn.cursor()

def createTables():
    cur.execute("CREATE DATABASE IF NOT EXISTS Assignment5;")   # Create the database
    cur.execute("USE Assignment5;")                             # Make the new database the selected database
    cur.execute("CREATE TABLE IF NOT EXISTS Movie ( id INT PRIMARY KEY, homepage VARCHAR(5000) CHARACTER SET utf8, budget BIGINT DEFAULT 0,original_language CHAR(2) CHARACTER SET utf8,original_title VARCHAR(5000) CHARACTER SET utf8,overview VARCHAR(5000) CHARACTER SET utf8,popularity FLOAT DEFAULT 0.0,release_date DATE DEFAULT NULL,revenue BIGINT DEFAULT 0,runtime INT DEFAULT 0,status VARCHAR(100) CHARACTER SET utf8,tagline VARCHAR(5000) CHARACTER SET utf8,title VARCHAR(1000) CHARACTER SET utf8,vote_average FLOAT DEFAULT 0.0, vote_count INT DEFAULT 0.0);")
    # Create the movie table
    cur.execute("CREATE TABLE IF NOT EXISTS genres(id INT PRIMARY KEY, name VARCHAR(1000) CHARACTER SET utf8);")
    # Create the Genre table sepparate from the movie table 
    cur.execute("CREATE TABLE IF NOT EXISTS keywords(id INT PRIMARY KEY, name VARCHAR(1000) CHARACTER SET utf8);")
    # Create the keyword table sepparate from the movie table 
    cur.execute("CREATE TABLE IF NOT EXISTS production_companies(id INT PRIMARY KEY, name VARCHAR(1000) CHARACTER SET utf8);")
    # Create the production_company table sepparate from the movie table 
    cur.execute("CREATE TABLE IF NOT EXISTS production_countries(iso_3166_1 CHAR(2) CHARACTER SET utf8 PRIMARY KEY , name VARCHAR(1000) CHARACTER SET utf8);")
    # Create the production_countries table sepparate from the movie table 
    cur.execute("CREATE TABLE IF NOT EXISTS spoken_languages(iso_639_1 CHAR(2) CHARACTER SET utf8 PRIMARY KEY, name VARCHAR(1000) CHARACTER SET utf8);")
    # Create the spoken_languages table sepparate from the movie table 
    cur.execute("CREATE TABLE IF NOT EXISTS genreToMovie (id INT AUTO_INCREMENT ,GID INT, MID INT, PRIMARY KEY(id), FOREIGN KEY(GID) REFERENCES genres(id), FOREIGN KEY(MID) REFERENCES Movie(id));")
    # Join table for genre and movie tables
    cur.execute("CREATE TABLE IF NOT EXISTS keywordToMovie (id INT AUTO_INCREMENT, PRIMARY KEY(id), KID INT, MID INT, FOREIGN KEY(KID) REFERENCES keywords(id), FOREIGN KEY(MID) REFERENCES Movie(id));")
    # Join table for company and movie tables
    cur.execute("CREATE TABLE IF NOT EXISTS companyToMovie(id INT AUTO_INCREMENT, PRIMARY KEY(id),CMPID INT, MID INT,FOREIGN KEY(CMPID) REFERENCES production_companies(id), FOREIGN KEY(MID) REFERENCES Movie(id));")
    # Join table for country and movie tables
    cur.execute("CREATE TABLE IF NOT EXISTS countryToMovie(id INT AUTO_INCREMENT, PRIMARY KEY(id),COUID CHAR(2) CHARACTER SET utf8, MID INT, FOREIGN KEY(COUID) REFERENCES production_countries(iso_3166_1), FOREIGN KEY(MID) REFERENCES Movie(id));")
    # Join table for spoken and movie tables
    cur.execute("CREATE TABLE IF NOT EXISTS spokenToMovie(id INT AUTO_INCREMENT, PRIMARY KEY(id), SID CHAR(2) CHARACTER SET utf8, MID INT, FOREIGN KEY(SID) REFERENCES spoken_languages(iso_639_1), FOREIGN KEY(MID) REFERENCES Movie(id));")

def parseCSV(cs):
    def loadDict(d, parsedjson, key):
        # Load the given dictionary with a list of dictionaries {key:{key:value}}
        # This allows you to quickly check for duplicates before insertion.
        new = []    # This list will populated with the new items (non duplicates) and returned.
        for i in range(0,len(parsedjson)):
            l = len(d)
            d[parsedjson[i][key]] = parsedjson[i]
            if len(d) != l: # Since the length actually changed upon insertion into the dictionary, must be a new item.
                new.append(parsedjson[i])
        return new
    def processLeJSON(d,key,index,row,table,joinTable,fkey):
        temp = json.loads(row[index])
        # Load the keywords from that cell into a dictionary and find the new ones to be inserted.
        jsonItems = loadDict(d,temp,key)
        insert = "INSERT INTO `"+table+"`("+"`"+key+"`"+", `name`) VALUES(%s,%s);"
        insert_join = "INSERT INTO"+"`"+joinTable+"`"+"(`"+fkey+"`"+",`MID`) VALUES(%s, %s)"
        for item in jsonItems:
            cur.execute(insert,(item[key],item['name']))
        for item in temp:
            cur.execute(insert_join,(item[key],row[3]))

    with open(cs,'r') as f:
        readcs = csv.reader(f)  # Parse the csv file
        # Initialize our dictionaries which will store all the unique keywords, companies, countries, genres, and languages.
        keywords = {}
        production_companies = {}
        production_countries = {}
        genres = {}
        spoken_languages = {}
        # Just a flag so I can skip the first row.
        i = False
        for row in readcs:

            if(not i):  # Skip first row.
                i = True
                continue
            # Deal with the random blank collumns in the database.
            if(row[13] == ''):
                row[13] = 0
            if(row[11] == ''):
                row[11] = datetime.date.today()
            # Create the string that will be run in MYSQL for inserting each movie.
            mov_insert = "INSERT INTO `Movie` (`id`, `homepage`, `budget`, `original_language`, `original_title`, `overview`, `popularity`, `release_date`, `revenue`, `runtime`, `status`, `tagline`, `title`, `vote_average`, `vote_count`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            # Execute that string and replace each %s with a value from the csv.
            cur.execute(mov_insert,(row[3],row[2],row[0],row[5],row[6],row[7],row[8],row[11],row[12],row[13],row[15],row[16],row[17],row[18],row[19]))
            # row[4] contains the keywords
            #def processLeJSON(d,key,index,row,table,joinTable,fkey):
            processLeJSON(keywords,"id",4,row,"keywords","keywordToMovie","KID")
            processLeJSON(production_companies,"id",9,row,"production_companies","companyToMovie", "CMPID")
            processLeJSON(production_countries,"iso_3166_1",10,row,"production_countries","countryToMovie","COUID")
            processLeJSON(genres,"id",1,row,"genres","genreToMovie","GID")
            processLeJSON(spoken_languages,"iso_639_1",14,row,"spoken_languages","spokenToMovie","SID")
        conn.commit()

def queries(queryNumbers):
    que = ["SELECT AVG(budget) FROM Movie;", "SELECT Movie.original_title, production_companies.name FROM (((Assignment5.Movie INNER JOIN countryToMovie ON countryToMovie.MID = Assignment5.Movie.id) INNER JOIN production_countries ON countryToMovie.COUID = production_countries.iso_3166_1) INNER JOIN companyToMovie ON companyToMovie.MID = Movie.id) INNER JOIN production_companies ON production_companies.id = companyToMovie.CMPID WHERE production_countries.iso_3166_1 = 'US';", "SELECT Movie.original_title, Movie.revenue FROM Movie ORDER BY Movie.revenue DESC LIMIT 5;", "SELECT Movie.original_title, genres.name FROM (Movie INNER JOIN genreToMovie ON Movie.id = genreToMovie.MID) INNER JOIN genres ON genres.id = genreToMovie.GID WHERE Movie.id IN (SELECT Movie.id From (Movie INNER JOIN genreToMovie ON Movie.id = genreToMovie.MID) INNER JOIN genres ON genres.id = genreToMovie.GID AND genres.name = 'Mystery') AND Movie.id IN (SELECT Movie.id From (Movie INNER JOIN genreToMovie ON Movie.id = genreToMovie.MID) INNER JOIN genres ON genres.id = genreToMovie.GID AND genres.name = 'Science Fiction');", "SELECT Movie.original_title, Movie.popularity FROM Movie WHERE Movie.popularity > (SELECT AVG(Movie.popularity) FROM Movie);"]
    for i in queryNumbers:
        count = 0
        cur.execute(que[i])
        print("###############!!!!!!!!!!#########QUERY "+str(i))
        for line in cur.fetchall():
            count += 1
            print(line)
            if(count > 4):
                break


createTables()
try:
    parseCSV("tmdb_5000_movies.csv")
except:
    print("Failed while trying to load in the data, will try queries anyway")
finally:
    if(arglen < 4):
        queries(range(0,5))
    else:
        queries([int(args[3])])

    conn.close()
    cur.close()
