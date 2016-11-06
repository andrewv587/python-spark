#!/usr/bin/python
# -*- coding:utf-8 -*- 
#Filename:template.py
#Function:spark-template
#Author:Huang Weihang
#Email:huangweihang14@mails.ucas.ac.cn
#Data:2016-11-05

#set defaultCode:
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

## Spark Application - execute with spark-submit
 
## Imports
from pyspark import SparkConf, SparkContext
from jieba import analyse as jieba_analyse
import gensim
  
## Module Constants
APP_NAME="mobile tag"

INPUTFILE="mobile_data.txt"
OUTPUTDIR="mobile_apk_tag"

STOPWORDS=["android","应用"]
#STOPWORD_FILE="../stopwords.txt" 
MODEL_FILE='vectorsAccrChnBrand.bin'
MODEL=gensim.models.Word2Vec.load_word2vec_format(MODEL_FILE, binary=True)
MODEL_TAGS_PATH="/user/zhengfu/conf/media_tag.conf"

word2tag_dict_t = dict()
tag2word_dict_t = dict()
TAG_LEVEL = {1:2, 2:5, 3:8}
LEVEL_NUM=3
VOTE_NUM=4
SIM_THRESHOLD=0.1

## Closure Functions
def getApkWordsPair(line):
    words=line.split("\t")
    apk=words[1].strip()
    tags=words[0].split(",")[:-2]
    desc="".join(words[2].split("：".decode("utf-8"))[1:])
    sentence=(" ".join(tags)+desc).lower()
    for stopword in STOPWORDS:
        sentence=sentence.replace(stopword.decode("utf-8"),"")
    #set stopwords,need to add file to all workers
    #jieba_analyse.set_stop_words(STOPWORD_FILE) 
    #use jieba to cacluate the top 10 IF-DIF words 
    tags_weight=jieba_analyse.extract_tags(sentence,topK=10,withWeight=True) 
    return (apk,tags_weight)

def getDict(list_t):
    for line in list_t:
        word, tag=line.split()
        word = word.encode('utf-8')
        word2tag_dict_t.setdefault(word, tag)
        tag2word_dict_t.setdefault(tag, word)
                                                                       
def inferDomainTag(line) :
	line = line.strip().split()
	domain = line[0]
	words = line[1:]
	final_tag_dict = dict()
	## method one : direct match in tag our system
	#flag = False
	#for word in words :
	#	if word in word2tag_dict :
	#		tag = word2tag_dict[word]
	#		feat = " ".join([domain, w, tag]).encode('utf-8')
	#		flag = True
	#if not flag :
	## method two : according to sim metric, choose one tag which get top score
	sim_dict = dict()
	feat_l = list()
	feat_l.append(domain)
	#result = model.most_similar(wordname1.decode("utf-8"),topn=20)
	for item in words :
		word, num = item
		for w in word2tag_dict :
			try :
				tag = word2tag_dict[w]
				sim = num * model.similarity(word, w.decode('utf-8'))
				feat = word + "|" + w.decode('utf-8') + "|" + tag
				sim_dict.setdefault(feat, sim)
			except :
				continue
	for n,kv in enumerate(sorted(sim_dict.items(), key = lambda x : x[1], reverse = True)) :
		if n == VOTE_NUM or kv[1]<SIM_THRESHOLD:
			break
		tag = kv[0].split("|")[-1][:tag_level[LEVEL_NUM]]
		final_tag_dict.setdefault(tag, 0)
		final_tag_dict[tag] += 1
		
	f_tag = sorted(final_tag_dict.items(), key = lambda x : x[1], reverse = True)
	if len(f_tag) > 0 :
		final_tag = f_tag[0][0]
		fea = " ".join([tag2word_dict[final_tag].decode('utf-8'), final_tag])
	else :
		fea = 'null'
		
	feat_l.append(fea)
	feat = " ".join(feat_l).encode('utf-8')

	return feat

def hdfsPathExists(path) :
    cmd = "hadoop fs -test -d %s" %(path)
    status = os.system(cmd)
    return status

def removeHdfsDir(path) :
    cmd = "hadoop fs -rm -r %s" %(path)
    status = os.system(cmd)
    return status


def main(sc):
   data=sc.textFile(INPUTFILE)
   apk_words=data.filter(lambda line:len(line)>20). \
           map(getApkWordsPair).reduceByKey(lambda a,b:a). \
           filter(lambda x:len(x[1])>2)
   outData=apk_words.take(100)
   for data in outData:
       tmpStr=data[0]+" "
       for k,v in data[1]:
           tmpStr+=k+" " #+":"+str(v)+" "
       print tmpStr

    list_t=sc.textFile(MODEL_TAGS_PATH).map(lambda line: line.strip()). \
            collect()
    getDict(list_t)
    word2tag_dict = sc.broadcast(word2tag_dict_t).value
    tag2word_dict = sc.broadcast(tag2word_dict_t).value
    apk_word_tag = apk_words.map(inferDomainTag).filter(lambda line:len(line)>2)
    if hdfsPathExists(res_path) == 0 :
        status = removeHdfsDir(res_path)
        if status != 0 :
            print "remove %s failed!" %(res_path)
    apk_word_tag.saveAsTextFile(OUTPUTDIR)
    print "output: %s" %(OUTPUTDIR)
    sc.stop()


if __name__ == "__main__":
    # Configure Spark
    conf=SparkConf().setAppName(APP_NAME)
    sc=SparkContext(conf=conf)
    # Execute Main functionality
    main(sc)
