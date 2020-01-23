import MySQLdb
import requests
import json
from multiprocessing.pool import ThreadPool as Pool
# from multiprocessing import Pool

pool_size = 8  # your "parallelness"
pool = Pool(pool_size)


db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     db="monarch")        # name of the data base

db.autocommit(True)
# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

#get all associations
geneDisease = []
cur.execute("SELECT * from `gene-disease`")
associations = cur.fetchall()
for link in associations:
    geneDisease.append((link[0],link[4]))

#get all the genes
cur.execute("SELECT distinct subject from `gene-disease`")
subjects = cur.fetchall()
# print all the first cell of all the rows
#for row in subjects[:5]:


def process_subjects(row):
    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()
    subject = row[0]
    print (subject)
    cur.execute("SELECT distinct object from `gene-phenotype` where subject = '"+subject+"'") #for each gene get the phenotype profile of said gene
    phenotypes = cur.fetchall()
    profile = "&id=".join(str(i[0]) for i in phenotypes) #create phenotypic profile for this object
    if len(profile) < 1: #if a profile does not exist go onto the next
        print ("no profile for " +subject)
        return True
    try:
        response = requests.get('http://owlsim3.monarchinitiative.org/api/match/phenodigm?id='+profile) # attempt to get the profile
    except:
        print ("Error opening "+'http://owlsim3.monarchinitiative.org/api/match/phenodigm?id='+profile)
        return True
    data = json.load(response)
    if "matches" in data and len(data['matches']) > 0: #if there is any matches then loop through them
        for r in data['matches']:
            if('matchId' not in r): #if there is no matchID returned then print out what is here
                print (r)
            print("setting score")
            if r['matchId'] != subject:
                if((subject,r['matchId']) in geneDisease): #if we already have this association then update its score, otherwise add to orphan table
                    sql = "UPDATE `gene-disease` SET S2O =%s WHERE subject=%s AND object = %s"
                    cur.execute(sql,(str(r['rawScore']),subject,r['matchId']))
                else:
                    sql = "INSERT INTO `gene-disease-orphan` (subject,subject_label,object,object_label,score) VALUES (%s,%s,%s,%s,%s)"
                    cur.execute(sql,(subject, data['matches'][0]['matchLabel'] ,r['matchId'] ,r['matchLabel'] , str(r['rawScore'])))

#get diseases
cur.execute("SELECT distinct object from `gene-disease`")
objects = cur.fetchall()
def process_objects(row):
    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()
    object = row[0]
    print(object)
    cur.execute("SELECT distinct object from `disease-phenotype` where subject = '" + object + "'")
    phenotypes = [str(pheno[0]) for pheno in cur.fetchall()]
    if not len(phenotypes):
        return True
    try:
        # Example query https://api.monarchinitiative.org/api/sim/search?id=MP%3A0000849&id=HP%3A0001765&limit=100&taxon=9606
        params = {
            'id': phenotypes,
            'metric': 'phenodigm',
            'limit': 10000,
            'taxon': '9606'
        }
        response = requests.get('https://api.monarchinitiative.org/api/sim/search', params=params)
    except:
        print ("Error opening api/sim/search with params: {}".format(params))
        return True
    data = json.load(response)
    if "matches" in data and len(data['matches']):
        for r in data['matches']:
            if ('id' not in r):
                print (r)
            if r['id'] is not object:
                if ((r['id'],object) in geneDisease):
                    sql = "UPDATE `gene-disease` SET O2S =%s WHERE object=%s AND subject = %s"
                    cur.execute(sql, (str(r['score']), object, r['id']))
                else:
                    sql = "INSERT INTO `gene-disease-orphan` (subject,subject_label,object,object_label,score) VALUES (%s,%s,%s,%s,%s)"
                    cur.execute(sql, (
                    object, data['matches'][0]['label'], r['id'], r['label'], str(r['score'])))

pool.map(process_subjects,subjects) #using parallel processing get matches for all subjects in gene-disease table
pool.map(process_objects,objects) #using parallel processing get matches for all objects in gene-disease table
db.close()