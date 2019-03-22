import pymysql
import json
import csv
import datetime
conn = pymysql.connect(host='localhost', port=3306, user='Andrew', passwd='123Saymoo')

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
            if len(d) != l: # New stuff
                new.append(parsedjson[i])
        return new
    with open(cs,'r') as f:
        readcs = csv.reader(f)
        keywords = {}
        production_companies = {}
        production_countries = {}
        genres = {}
        spoken_languages = {}
        i = False
        count = 0
        for row in readcs:
            curr = row;
            if(not i):
                i = True
                continue
            mov_insert = "INSERT INTO `Movie` (`id`, `homepage`, `budget`, `original_language`, `original_title`, `overview`, `popularity`, `release_date`, `revenue`, `runtime`, `status`, `tagline`, `title`, `vote_average`, `vote_count`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            if(curr[13] == ''):
                curr[13] = 0
            if(curr[11] == ''):
                curr[11] = datetime.date.today()
            cur.execute(mov_insert,(curr[3],curr[2],curr[0],curr[5],curr[6],curr[7],curr[8],curr[11],curr[12],curr[13],curr[15],curr[16],curr[17],curr[18],curr[19]))
            temp_kw = json.loads(curr[4])
            newKeywords = loadDict(keywords,temp_kw,"id")
            key_insert = "INSERT INTO `keywords`(`id`, `name`) VALUES(%s,%s);"
            for key in newKeywords:
                cur.execute(key_insert,(key['id'],key['name']));
            # Add to the join table after you add the new data to the keyword table.    
            temp_comp = json.loads(curr[9])
            newComp = loadDict(production_companies,temp_comp,"id")
            prod_insert = "INSERT INTO `production_companies`(`id`, `name`) VALUES(%s,%s);"
            for key in newComp:
                cur.execute(prod_insert,(key['id'],key['name']));
            temp_cou = json.loads(curr[10])
            newCountries = loadDict(production_countries,temp_cou,"iso_3166_1")
            cou_insert = "INSERT INTO `production_countries`(`iso_3166_1`, `name`) VALUES(%s,%s);"
            for key in newCountries:
                cur.execute(cou_insert,(key['iso_3166_1'],key['name']));
            temp_gen = json.loads(curr[1])
            newGenres = loadDict(genres,temp_gen,"id")
            gen_insert = "INSERT INTO `genres`(`id`, `name`) VALUES(%s,%s);"
            for key in newGenres:
                cur.execute(gen_insert,(key['id'],key['name']));
            temp_spok = json.loads(curr[14])
            newSpoken = loadDict(spoken_languages,temp_spok,"iso_639_1")
            spok_insert = "INSERT INTO `spoken_languages`(`iso_639_1`, `name`) VALUES(%s,%s);"
            for key in newSpoken:
                cur.execute(spok_insert,(key['iso_639_1'],key['name']));
        conn.commit()

createTables()
parseCSV("tmdb_5000_movies.csv")

conn.close()
cur.close()
